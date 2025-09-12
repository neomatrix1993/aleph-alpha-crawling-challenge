import io
import json
from prometheus_client import start_http_server
import trafilatura
from warcio.archiveiterator import WARCIterator
from prometheus_client import Counter

from commoncrawl import BASE_URL, CCDownloader, Downloader
from rabbitmq import QUEUE_NAME, rabbitmq_channel


batch_counter = Counter("worker_batches", "Number of consumed batches")
warc_files_downloaded = Counter("worker_warc_files_downloaded", "Number of WARC files downloaded")
warc_records_total = Counter("worker_warc_records_total", "Total WARC records processed")
warc_records_response = Counter("worker_warc_records_response", "WARC records that are HTTP responses")
text_extractions_attempted = Counter("worker_text_extractions_attempted", "Text extractions attempted")
text_extractions_successful = Counter("worker_text_extractions_successful", "Text extractions that produced non-empty text")


def process_batch(downloader: Downloader, ch, method, _properties, body):
    print("Received batch of size", len(body))
    batch = json.loads(body)
    for item in batch:
        data = downloader.download_and_unzip(
            item["metadata"]["filename"],
            int(item["metadata"]["offset"]),
            int(item["metadata"]["length"]),
        )
        warc_files_downloaded.inc()
        
        for record in WARCIterator(io.BytesIO(data)):
            warc_records_total.inc()
            if record.rec_type == "response":
                warc_records_response.inc()
                text_extractions_attempted.inc()
                _text = trafilatura.extract(record.content_stream().read())
                if _text and _text.strip():
                    text_extractions_successful.inc()
                # TODO: process text
    batch_counter.inc()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    start_http_server(9001)
    downloader = CCDownloader(BASE_URL)
    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, ch, method, properties, body
        ),
    )
    channel.start_consuming()


if __name__ == "__main__":
    main()
