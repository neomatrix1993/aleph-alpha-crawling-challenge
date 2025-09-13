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


def main() -> None:
    args = parse_args()
    start_http_server(9000)
    channel = RabbitMQChannel()
    crawl_path = get_crawl_path(args.crawl_version)
    downloader = CCDownloader(f"{BASE_URL}/{crawl_path}")
    index_reader = CSVIndexReader(args.cluster_idx_filename)
    process_index(index_reader, channel, downloader, BATCH_SIZE)


if __name__ == "__main__":
    main()
