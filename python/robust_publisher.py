import json
from typing import Callable, List, Optional, Any
from prometheus_client import Counter

from checkpoint import ProcessingCheckpoint
from health_checker import ConnectionHealthChecker


# Prometheus metrics for monitoring
connection_failures = Counter("batcher_connection_failures", "RabbitMQ connection failures")
publish_retries = Counter("batcher_publish_retries", "RabbitMQ publish retry attempts")
checkpoint_saves = Counter("batcher_checkpoint_saves", "Number of checkpoint saves")
messages_published = Counter("batcher_messages_published", "Total messages published to RabbitMQ")


class RobustPublisher:
    """Handles robust message publishing with automatic retry and checkpointing.
    
    Combines connection health checking, exponential backoff, and persistent
    checkpointing to ensure reliable message delivery even during outages.
    """
    
    def __init__(self, channel_factory: Callable, checkpoint: Optional[ProcessingCheckpoint],
                 queue_name: str = "crawl_batches"):
        self.channel_factory = channel_factory
        self.checkpoint = checkpoint
        self.queue_name = queue_name
        self.health_checker = ConnectionHealthChecker()
        self.channel = None
        
    def ensure_connection(self) -> bool:
        """Establish or verify RabbitMQ connection.
        
        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            # Create new channel if needed
            if not self.channel:
                self.channel = self.channel_factory()
                print("🔗 New RabbitMQ connection established")
                
            # Test connection health with a lightweight operation
            # Note: We'll just check if channel is open rather than doing operations
            # that might interfere with queue state
            if self.channel.channel.is_open:
                self.health_checker.record_success()
                return True
            else:
                raise Exception("Channel is closed")
                
        except Exception as e:
            print(f"🔴 RabbitMQ connection test failed: {e}")
            self.channel = None
            self.health_checker.record_failure()
            connection_failures.inc()
            return False
    
    def publish_with_recovery(self, batch: List[Any], line_number: int, batch_count: int) -> Optional[bool]:
        """Publish batch with automatic retry and recovery.
        
        Args:
            batch: List of items to publish
            line_number: Current line number in processing
            batch_count: Current batch count
            
        Returns:
            True: Successfully published
            False: Failed, should retry
            None: Failed permanently, should exit
        """
        
        # Check if we should attempt connection
        if not self.ensure_connection():
            if self.health_checker.should_retry():
                self.health_checker.wait_with_backoff()
                publish_retries.inc()
                return False  # Signal caller to retry
            else:
                print("❌ Max connection attempts reached. Giving up.")
                return None  # Signal caller to exit
        
        try:
            # Attempt to publish the batch
            message_body = json.dumps(batch)
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message_body
            )
            
            # Success! Update metrics and checkpoint
            messages_published.inc()
            if self.checkpoint:
                self.checkpoint.save_progress(line_number, batch_count)
                checkpoint_saves.inc()
            
            print(f"✅ Published batch {batch_count} ({len(batch)} items) - line {line_number}")
            return True
            
        except Exception as e:
            print(f"🔴 Publish failed: {e}")
            # Force reconnection on next attempt
            self.channel = None
            self.health_checker.record_failure()
            
            if self.health_checker.should_retry():
                self.health_checker.wait_with_backoff()
                publish_retries.inc()
                return False  # Signal caller to retry
            else:
                print("❌ Max publish attempts reached. Giving up.")
                return None  # Signal caller to exit
    
    def get_status(self) -> dict:
        """Get current publisher status for monitoring.
        
        Returns:
            Dict with connection and health status information
        """
        return {
            "connected": self.channel is not None and self.channel.channel.is_open if self.channel else False,
            "health_status": self.health_checker.get_status(),
            "queue_name": self.queue_name
        }
    
    def close(self) -> None:
        """Clean up resources."""
        if self.channel:
            try:
                self.channel.close()
                print("🔌 RabbitMQ connection closed")
            except Exception as e:
                print(f"⚠️ Error closing connection: {e}")
            finally:
                self.channel = None