import json
import os
import time
from typing import Dict


class ProcessingCheckpoint:
    """Manages persistent checkpointing for batcher processing state.
    
    Saves progress to disk to enable resumption after failures or restarts.
    """
    
    def __init__(self, checkpoint_file: str = "batcher_checkpoint.json"):
        self.checkpoint_file = checkpoint_file
        
    def save_progress(self, line_number: int, batch_count: int) -> None:
        """Save current processing progress to disk.
        
        Args:
            line_number: Last successfully processed line from cluster.idx
            batch_count: Total number of batches successfully sent to RabbitMQ
        """
        checkpoint = {
            "last_processed_line": line_number,
            "total_batches_sent": batch_count,
            "timestamp": time.time()
        }
        
        # Atomic write to prevent corruption during crashes
        temp_file = f"{self.checkpoint_file}.tmp"
        try:
            with open(temp_file, 'w') as f:
                json.dump(checkpoint, f, indent=2)
            os.rename(temp_file, self.checkpoint_file)
            print(f"Checkpoint saved: line {line_number}, batches {batch_count}")
        except (IOError, OSError) as e:
            print(f"Failed to save checkpoint: {e}")
        finally:
            # Clean up temp file if it exists
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except OSError:
                    pass
                    
    def load_progress(self) -> Dict[str, int]:
        """Load processing progress from disk.
        
        Returns:
            Dict with keys: last_processed_line, total_batches_sent, timestamp
            Returns defaults if no checkpoint file exists or is corrupted.
        """
        default_progress = {
            "last_processed_line": 0,
            "total_batches_sent": 0,
            "timestamp": 0
        }
        
        if not os.path.exists(self.checkpoint_file):
            print("No checkpoint found, starting from beginning")
            return default_progress
            
        try:
            with open(self.checkpoint_file, 'r') as f:
                progress = json.load(f)
                
            # Validate checkpoint structure
            required_keys = ["last_processed_line", "total_batches_sent"]
            if all(key in progress for key in required_keys):
                print(f"Checkpoint loaded: resuming from line {progress['last_processed_line']}")
                return progress
            else:
                print("Invalid checkpoint format, starting from beginning")
                return default_progress
                
        except (json.JSONDecodeError, IOError) as e:
            print(f"Corrupted checkpoint file ({e}), starting from beginning")
            return default_progress
            
    def clear_checkpoint(self) -> None:
        """Remove checkpoint file (useful for fresh starts)."""
        if os.path.exists(self.checkpoint_file):
            try:
                os.remove(self.checkpoint_file)
                print("🗑️ Checkpoint file cleared")
            except OSError as e:
                print(f"⚠️ Failed to clear checkpoint: {e}")