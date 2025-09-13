from abc import ABC, abstractmethod
import json
import argparse
from typing import Any, Mapping, Sequence
from prometheus_client import Counter, start_http_server

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


BATCH_SIZE = 50

batch_counter = Counter("batcher_batches", "Number of published batches")
urls_total = Counter("batcher_urls_total", "Total URLs processed from index files")
urls_filtered_language = Counter("batcher_urls_filtered_language", "URLs filtered out (non-English)")
urls_filtered_status = Counter("batcher_urls_filtered_status", "URLs filtered out (non-200 status)")
urls_kept = Counter("batcher_urls_kept", "URLs that passed all filters")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batcher")
    parser.add_argument(
        "--cluster-idx-filename", type=str, help="Input file path", required=True
    )
    parser.add_argument(
        "--crawl-version", type=str, default="CC-MAIN-2024-30",
        help="Common Crawl version (default: CC-MAIN-2024-30)"
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
) -> bool:
    """Process index with robust publishing and resume capability."""
    print(f"Starting processing from line {resume_from_line}, batch size {batch_size}")
    
    found_urls = []
    current_line = 0
    batch_count = 0
    chunk_count = 0
    urls_skipped = 0
    
    try:
        for cdx_chunk in index:
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
                    
                # URL passed filters
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


def main() -> None:
    args = parse_args()
    print(f"Starting batcher with crawl version: {args.crawl_version}")
    print(f"Index file: {args.cluster_idx_filename}")
    
    start_http_server(9000)
    print("Prometheus metrics server started on port 9000")
    
    # Setup checkpoint and recovery system
    checkpoint = ProcessingCheckpoint()
    progress = checkpoint.load_progress()
    print(f"Loaded checkpoint: {progress['total_batches_sent']} batches sent, line {progress['last_processed_line']}")
    
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
    
    # Process with recovery
    try:
        success = process_index_with_recovery(
            index_reader, publisher, downloader, BATCH_SIZE,
            resume_from_line=progress["last_processed_line"]
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


if __name__ == "__main__":
    main()
