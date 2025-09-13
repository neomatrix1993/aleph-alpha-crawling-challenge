from abc import ABC, abstractmethod
import json
import argparse
from typing import Any, Mapping, Sequence, List
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from prometheus_client import Counter, Gauge, start_http_server

from commoncrawl import (
    BASE_URL,
    get_crawl_path,
    CCDownloader,
    CSVIndexReader,
    Downloader,
    IndexReader,
)
from rabbitmq import QUEUE_NAME, MessageQueueChannel, RabbitMQChannel
from checkpoint import ProcessingCheckpoint
from robust_publisher import RobustPublisher
from multi_crawl import (
    URLDeduplicator,
    CrawlCheckpointManager,
    ClusterIndexDownloader,
    parse_crawl_versions,
)


BATCH_SIZE = 50

batch_counter = Counter("batcher_batches", "Number of published batches")
urls_total = Counter("batcher_urls_total", "Total URLs processed from index files")
urls_filtered_language = Counter("batcher_urls_filtered_language", "URLs filtered out (non-English)")
urls_filtered_status = Counter("batcher_urls_filtered_status", "URLs filtered out (non-200 status)")
urls_kept = Counter("batcher_urls_kept", "URLs that passed all filters")

# Progress monitoring metrics
processing_progress_percent = Gauge("batcher_progress_percent", "Percentage of cluster.idx file processed")
total_index_lines = Gauge("batcher_total_index_lines", "Total estimated lines in cluster.idx")
batches_published_total = Gauge("batcher_batches_published_total", "Total batches published so far")


def get_total_lines_from_index(filename: str) -> int:
    """Get exact total lines by reading the last line number from cluster.idx"""
    try:
        with open(filename, 'rb') as f:
            # Seek to end and read backwards to find last line
            f.seek(-1000, 2)  # Go to near end of file
            lines = f.read().decode('utf-8').split('\n')
            
            # Find the last non-empty line
            for line in reversed(lines):
                if line.strip():
                    parts = line.split('\t')
                    if len(parts) >= 5:  # Ensure we have enough columns
                        try:
                            total_lines = int(parts[-1])  # Last column has line number
                            print(f"Found {total_lines:,} total lines in cluster.idx")
                            return total_lines
                        except ValueError:
                            continue
            
        print("Could not parse line numbers from cluster.idx")
        return 0
        
    except Exception as e:
        print(f"Could not read cluster.idx file: {e}")
        return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Multi-crawl batcher with URL deduplication"
    )
    
    # For backward compatibility, support both old and new argument formats
    parser.add_argument(
        "--cluster-idx-filename", 
        type=str, 
        help="Input file path (deprecated, use --crawl-versions instead)", 
        required=False
    )
    parser.add_argument(
        "--crawl-version", 
        type=str, 
        help="Single crawl version (deprecated, use --crawl-versions instead)"
    )
    parser.add_argument(
        "--crawl-versions", 
        type=str, 
        default="CC-MAIN-2024-30",
        help="Comma-separated crawl versions (e.g., CC-MAIN-2024-30,CC-MAIN-2024-33)"
    )
    
    return parser.parse_args()


def publish_batch(
    channel: MessageQueueChannel,
    batch: Sequence[Mapping[str, Any]],
) -> None:
    print("Pushing batch of size", len(batch))
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(batch),
    )
    batch_counter.inc()


def process_index(
    index: IndexReader,
    channel: MessageQueueChannel,
    downloader: Downloader,
    batch_size: int,
) -> None:
    found_urls = []
    for cdx_chunk in index:
        data = downloader.download_and_unzip(
            cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
        ).decode("utf-8")
        for line in data.split("\n"):
            if line == "":
                continue
            urls_total.inc()
            values = line.split(" ")
            metadata = json.loads("".join(values[2:]))
            
            # Check language filter
            if not ("languages" in metadata and "eng" in metadata["languages"]):
                urls_filtered_language.inc()
                continue
                
            # Check status filter
            if metadata["status"] != "200":
                urls_filtered_status.inc()
                continue
                
            # URL passed all filters
            urls_kept.inc()
            found_urls.append(
                {
                    "surt_url": values[0],
                    "timestamp": values[1],
                    "metadata": metadata,
                }
            )
            if len(found_urls) >= batch_size:
                publish_batch(channel, found_urls)
                found_urls = []

    if len(found_urls) > 0:
        publish_batch(channel, found_urls)


def process_index_with_recovery(
    index: IndexReader,
    publisher: RobustPublisher,
    downloader: Downloader,
    batch_size: int,
    resume_from_line: int = 0,
    total_lines: int = 0,
    url_deduplicator: URLDeduplicator = None,
) -> bool:
    """Process index with robust publishing and resume capability."""
    print(f"Starting processing from line {resume_from_line}, batch size {batch_size}")
    
    found_urls = []
    current_line = 0
    batch_count = 0
    chunk_count = 0
    urls_skipped = 0
    processing_complete = False
    
    try:
        for cdx_chunk in index:
            if processing_complete:
                break
            chunk_count += 1
            try:
                data = downloader.download_and_unzip(
                    cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
                ).decode("utf-8")
                print(f"Processing chunk {chunk_count}: {cdx_chunk[1]}")
            except Exception as e:
                print(f"Failed to download chunk {chunk_count} ({cdx_chunk[1]}): {e}")
                continue
                
            for line in data.split("\n"):
                if line == "":
                    continue
                    
                current_line += 1
                
                # Exit if we've processed all lines in the file
                if total_lines > 0 and current_line > total_lines:
                    print(f"📄 Reached end of file at line {total_lines}, stopping processing")
                    processing_complete = True
                    break
                
                # Skip lines until resume position
                if current_line <= resume_from_line:
                    urls_skipped += 1
                    continue
                
                urls_total.inc()
                values = line.split(" ")
                try:
                    metadata = json.loads("".join(values[2:]))
                except json.JSONDecodeError:
                    continue
                
                # Apply filters
                if not ("languages" in metadata and "eng" in metadata["languages"]):
                    urls_filtered_language.inc()
                    continue
                    
                if metadata["status"] != "200":
                    urls_filtered_status.inc()
                    continue
                
                # Apply URL deduplication if provided
                if url_deduplicator and not url_deduplicator.should_process_url(values[0]):
                    continue
                    
                # URL passed all filters including deduplication
                urls_kept.inc()
                found_urls.append({
                    "surt_url": values[0],
                    "timestamp": values[1],
                    "metadata": metadata,
                })
                
                # Publish when batch full
                if len(found_urls) >= batch_size:
                    print(f"Publishing batch {batch_count + 1} with {len(found_urls)} URLs (line {current_line})")
                    while True:
                        result = publisher.publish_with_recovery(
                            found_urls, current_line, batch_count + 1
                        )
                        
                        if result is True:
                            found_urls = []
                            batch_count += 1
                            batches_published_total.set(batch_count)
                            
                            # Update progress percentage
                            if total_lines > 0:
                                progress = (current_line / total_lines) * 100
                                processing_progress_percent.set(progress)
                            break
                        elif result is False:
                            print(f"Retrying batch {batch_count + 1} publish")
                            continue
                        else:
                            print(f"Failed to publish batch {batch_count + 1}, stopping")
                            return False
        
        # Handle final partial batch
        if found_urls:
            print(f"Publishing final batch with {len(found_urls)} URLs")
            while True:
                result = publisher.publish_with_recovery(
                    found_urls, current_line, batch_count + 1
                )
                if result is True:
                    batch_count += 1
                    batches_published_total.set(batch_count)
                    break
                elif result is False:
                    print("Retrying final batch publish")
                    continue
                else:
                    print("Failed to publish final batch")
                    return False
        
        print(f"Processing completed: {batch_count} batches sent, {current_line} lines processed")
        if urls_skipped > 0:
            print(f"Skipped {urls_skipped} lines during resume")
        return True
        
    except KeyboardInterrupt:
        print(f"Processing interrupted at line {current_line}, {batch_count} batches sent")
        print("Checkpoint saved, run again to resume")
        return False


def process_single_crawl(
    crawl_version: str,
    checkpoint_manager: CrawlCheckpointManager,
    url_deduplicator: URLDeduplicator,
    cluster_downloader: ClusterIndexDownloader,
    publisher: RobustPublisher,
) -> bool:
    """
    Process a single crawl version with deduplication and checkpointing.
    
    Args:
        crawl_version: Common Crawl version to process
        checkpoint_manager: Manager for crawl checkpoints
        url_deduplicator: URL deduplication handler
        cluster_downloader: Cluster index downloader
        publisher: Robust message publisher
        
    Returns:
        True if crawl completed successfully, False otherwise
    """
    print(f"\n🚀 Processing crawl: {crawl_version}")
    
    # Load checkpoint for this crawl
    checkpoint = checkpoint_manager.load_checkpoint(crawl_version)
    
    # Check if already completed
    if checkpoint.status == 'completed':
        print(f"✅ Crawl {crawl_version} already completed, skipping")
        return True
    
    try:
        # Download cluster.idx for this crawl
        cluster_idx_path = cluster_downloader.download_cluster_idx(crawl_version)
        
        # Update checkpoint with cluster index filename
        checkpoint.cluster_idx_filename = str(cluster_idx_path)
        checkpoint.status = 'in_progress'
        if not checkpoint.start_time:
            checkpoint.start_time = datetime.now(timezone.utc).isoformat()
        checkpoint.last_update = datetime.now(timezone.utc).isoformat()
        
        # Get total lines from cluster.idx
        total_lines = get_total_lines_from_index(str(cluster_idx_path))
        checkpoint.total_lines = total_lines
        total_index_lines.set(total_lines)
        
        print(f"📊 Progress: {checkpoint.last_processed_line:,}/{total_lines:,} lines "
              f"({checkpoint.last_processed_line/total_lines*100:.1f}% complete)")
        
        # Setup downloader and index reader
        crawl_path = get_crawl_path(crawl_version)
        downloader_url = f"{BASE_URL}/{crawl_path}"
        downloader = CCDownloader(downloader_url)
        index_reader = CSVIndexReader(str(cluster_idx_path))
        
        # Create a thread-safe checkpoint wrapper for this crawl
        class CrawlSpecificCheckpoint:
            def __init__(self, checkpoint_manager, crawl_checkpoint):
                self.manager = checkpoint_manager
                self.checkpoint = crawl_checkpoint
                self._lock = threading.Lock()  # Thread safety for parallel processing
            
            def save_progress(self, line_number: int, batch_count: int):
                with self._lock:  # Ensure thread-safe checkpoint updates
                    self.checkpoint.last_processed_line = line_number
                    self.checkpoint.total_batches_sent = batch_count
                    self.checkpoint.last_update = datetime.now(timezone.utc).isoformat()
                    self.manager.save_checkpoint(self.checkpoint)
        
        crawl_checkpoint = CrawlSpecificCheckpoint(checkpoint_manager, checkpoint)
        
        # Wrap publisher to update crawl-specific checkpoint with bounds checking
        class CrawlAwarePublisher:
            def __init__(self, publisher, crawl_checkpoint, total_lines):
                self.publisher = publisher
                self.crawl_checkpoint = crawl_checkpoint
                self.total_lines = total_lines
            
            def publish_with_recovery(self, batch, line_number, batch_number):
                result = self.publisher.publish_with_recovery(batch, line_number, batch_number)
                if result is True:
                    # Ensure line number doesn't exceed file bounds
                    safe_line_number = min(line_number, self.total_lines)
                    # Update crawl-specific checkpoint on successful publish
                    self.crawl_checkpoint.save_progress(safe_line_number, batch_number)
                return result
            
            def close(self):
                if hasattr(self.publisher, 'close'):
                    self.publisher.close()
        
        crawl_publisher = CrawlAwarePublisher(publisher, crawl_checkpoint, total_lines)
        
        try:
            # Process with recovery and deduplication
            success = process_index_with_recovery(
                index_reader, 
                crawl_publisher, 
                downloader, 
                BATCH_SIZE,
                resume_from_line=checkpoint.last_processed_line,
                total_lines=total_lines,
                url_deduplicator=url_deduplicator
            )
            
            if success:
                # Mark crawl as completed
                checkpoint.status = 'completed'
                checkpoint.last_update = datetime.now(timezone.utc).isoformat()
                checkpoint_manager.save_checkpoint(checkpoint)
                
                print(f"✅ Completed crawl {crawl_version}: "
                      f"{checkpoint.total_batches_sent} batches sent")
                return True
            else:
                print(f"⚠️ Crawl {crawl_version} stopped due to errors")
                return False
        finally:
            # Ensure thread-specific publisher is properly closed
            crawl_publisher.close()
            
    except Exception as e:
        print(f"❌ Error processing crawl {crawl_version}: {e}")
        # Update checkpoint with error status
        checkpoint.status = 'error'
        checkpoint.last_update = datetime.now(timezone.utc).isoformat()
        checkpoint_manager.save_checkpoint(checkpoint)
        return False


def process_multiple_crawls(crawl_versions: List[str]) -> None:
    """
    Process multiple crawl versions sequentially with URL deduplication.
    
    Args:
        crawl_versions: List of crawl versions to process
    """
    print(f"🔄 Starting multi-crawl processing for {len(crawl_versions)} crawls:")
    for i, version in enumerate(crawl_versions, 1):
        print(f"  {i}. {version}")
    
    # Initialize components
    url_deduplicator = URLDeduplicator()
    checkpoint_manager = CrawlCheckpointManager()
    cluster_downloader = ClusterIndexDownloader()
    
    # Show initial status
    statuses = checkpoint_manager.list_crawl_statuses(crawl_versions)
    print(f"\n📋 Initial crawl statuses:")
    for version, status in statuses.items():
        print(f"  {version}: {status}")
    
    # Channel factory for robust publisher
    def create_channel():
        print("Creating new RabbitMQ channel")
        return RabbitMQChannel()
    
    successful_crawls = 0
    failed_crawls = 0
    
    try:
        # Process crawls in parallel using ThreadPoolExecutor
        print(f"🚀 Starting parallel processing of {len(crawl_versions)} crawls...")
        
        with ThreadPoolExecutor(max_workers=min(len(crawl_versions), 4)) as executor:
            # Submit all crawl processing tasks
            future_to_crawl = {}
            for crawl_version in crawl_versions:
                # Create crawl-specific queue name
                crawl_queue = f"{QUEUE_NAME}_{crawl_version}"
                print(f"📮 Crawl {crawl_version} will publish to queue: {crawl_queue}")
                
                # Each thread gets its own publisher with its own queue
                # Don't use ProcessingCheckpoint in multi-crawl mode (we use CrawlCheckpointManager instead)
                thread_publisher = RobustPublisher(create_channel, None, crawl_queue)
                
                future = executor.submit(
                    process_single_crawl,
                    crawl_version,
                    checkpoint_manager,
                    url_deduplicator,
                    cluster_downloader,
                    thread_publisher
                )
                future_to_crawl[future] = crawl_version
            
            # Process results as they complete
            for future in as_completed(future_to_crawl):
                crawl_version = future_to_crawl[future]
                try:
                    success = future.result()
                    if success:
                        successful_crawls += 1
                        print(f"✅ Crawl {crawl_version} completed successfully")
                    else:
                        failed_crawls += 1
                        print(f"❌ Crawl {crawl_version} failed")
                except Exception as e:
                    failed_crawls += 1
                    print(f"💥 Crawl {crawl_version} crashed: {e}")
                
                # Show current deduplication stats
                stats = url_deduplicator.get_stats()
                print(f"📊 Current deduplication stats: {stats['unique_urls']:,} unique URLs, "
                      f"{stats['duplicates_skipped']:,} duplicates skipped")
    
    finally:
        # Publishers are closed by individual threads
        pass
    
    # Final summary
    print(f"\n🏁 Multi-crawl processing completed:")
    print(f"  ✅ Successful: {successful_crawls}/{len(crawl_versions)}")
    print(f"  ❌ Failed: {failed_crawls}/{len(crawl_versions)}")
    
    final_stats = url_deduplicator.get_stats()
    print(f"  📊 Final deduplication stats: {final_stats['unique_urls']:,} unique URLs, "
          f"{final_stats['duplicates_skipped']:,} duplicates skipped")


def main() -> None:
    args = parse_args()
    
    # Start Prometheus metrics server
    start_http_server(9000)
    print("Prometheus metrics server started on port 9000")
    
    # Determine crawl versions to process
    if args.crawl_versions:
        # New multi-crawl mode
        try:
            crawl_versions = parse_crawl_versions(args.crawl_versions)
            print(f"\n🎯 Multi-crawl mode: processing {len(crawl_versions)} crawls")
            process_multiple_crawls(crawl_versions)
        except ValueError as e:
            print(f"❌ Error parsing crawl versions: {e}")
            return
    
    elif args.cluster_idx_filename and args.crawl_version:
        # Legacy single-crawl mode for backward compatibility
        print(f"\n🔄 Legacy mode: processing single crawl {args.crawl_version}")
        print(f"Index file: {args.cluster_idx_filename}")
        
        # Get total lines from cluster.idx
        total_lines = get_total_lines_from_index(args.cluster_idx_filename)
        total_index_lines.set(total_lines)
        
        # Setup checkpoint and recovery system
        checkpoint = ProcessingCheckpoint()
        progress = checkpoint.load_progress()
        print(f"Loaded checkpoint: {progress['total_batches_sent']} batches sent, line {progress['last_processed_line']}")
        
        # Set initial progress metrics
        batches_published_total.set(progress['total_batches_sent'])
        if total_lines > 0:
            initial_progress = (progress['last_processed_line'] / total_lines) * 100
            processing_progress_percent.set(initial_progress)
            print(f"Progress: {initial_progress:.1f}% complete")
        
        # Channel factory for robust publisher
        def create_channel():
            print("Creating new RabbitMQ channel")
            return RabbitMQChannel()
        
        publisher = RobustPublisher(create_channel, checkpoint, QUEUE_NAME)
        
        # Setup downloader and index reader
        crawl_path = get_crawl_path(args.crawl_version)
        downloader_url = f"{BASE_URL}/{crawl_path}"
        print(f"Using Common Crawl URL: {downloader_url}")
        
        downloader = CCDownloader(downloader_url)
        index_reader = CSVIndexReader(args.cluster_idx_filename)
        
        # Process with recovery (no deduplication in legacy mode)
        try:
            success = process_index_with_recovery(
                index_reader, publisher, downloader, BATCH_SIZE,
                resume_from_line=progress["last_processed_line"],
                total_lines=total_lines,
                url_deduplicator=None  # No deduplication in legacy mode
            )
            
            if success:
                print("Batcher completed successfully")
                checkpoint.clear_checkpoint()
            else:
                print("Batcher stopped due to errors")
                
        except Exception as e:
            print(f"Unexpected error in batcher: {e}")
        finally:
            publisher.close()
    
    else:
        print("❌ Error: Please provide either --crawl-versions or both --cluster-idx-filename and --crawl-version")
        print("Examples:")
        print("  # Multi-crawl mode (recommended):")
        print("  python batcher.py --crawl-versions CC-MAIN-2024-30,CC-MAIN-2024-33")
        print("  # Legacy single-crawl mode:")
        print("  python batcher.py --cluster-idx-filename cluster.idx --crawl-version CC-MAIN-2024-30")


if __name__ == "__main__":
    main()
