import io
import json
import os
import time
import pandas as pd
import boto3
from botocore.client import Config
from prometheus_client import start_http_server
import trafilatura
from transformers import AutoTokenizer
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
download_errors = Counter("worker_download_errors", "WARC download failures", ["error_type"])
batch_errors = Counter("worker_batch_errors", "Batches that failed processing")
items_skipped = Counter("worker_items_skipped", "Items skipped due to errors")
documents_processed = Counter("worker_documents_processed", "Documents successfully processed and tokenized")
documents_uploaded = Counter("worker_documents_uploaded", "Documents uploaded to object store")
tokenization_errors = Counter("worker_tokenization_errors", "Tokenization failures")


class DocumentBatch:
    """Manages batching of documents for upload to object store"""
    # change this to 50, 1, 1 for testing.
    def __init__(self, max_docs=10000, max_size_mb=256, max_time_minutes=30):
        self.documents = []
        self.max_docs = max_docs
        self.max_size_mb = max_size_mb
        self.max_time_minutes = max_time_minutes
        self.start_time = time.time()
        self.total_size = 0
    
    def add_document(self, doc):
        self.documents.append(doc)
        # Rough size estimation (characters * 1.5 for JSON overhead)
        self.total_size += len(doc.get('full_text', '')) * 1.5
    
    def should_upload(self):
        return (
            len(self.documents) >= self.max_docs or
            self.total_size >= self.max_size_mb * 1024 * 1024 or
            time.time() - self.start_time >= self.max_time_minutes * 60
        )
    
    def reset(self):
        self.documents = []
        self.total_size = 0
        self.start_time = time.time()


# Global tokenizer and document batch
tokenizer = None
document_batch = DocumentBatch(max_docs=10000, max_size_mb=256, max_time_minutes=30)  # Production batch size
s3_client = None


def upload_documents_to_minio(documents, bucket_name='processed-docs'):
    """Upload documents to MinIO as Parquet file"""
    global s3_client
    if not s3_client:
        return
    
    try:
        # Create DataFrame
        df = pd.DataFrame(documents)
        
        # Generate filename with timestamp
        timestamp = int(time.time())
        filename = f"processed/{time.strftime('%Y/%m/%d')}/batch_{timestamp}.parquet"
        
        # Convert to Parquet bytes
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, compression='gzip', index=False)
        parquet_bytes = parquet_buffer.getvalue()
        
        # Upload to MinIO
        s3_client.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=parquet_bytes,
            ContentType='application/octet-stream'
        )
        
        documents_uploaded.inc(len(documents))
        print(f"✅ Uploaded {len(documents)} documents to {filename}")
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")


def process_and_tokenize_text(text, url, timestamp, filename):
    """Process text with tokenization and create document record"""
    global tokenizer
    
    try:
        # Tokenize the text
        tokens = tokenizer.encode(text, max_length=512, truncation=True)
        
        # Create document record
        doc = {
            'url': url,
            'timestamp': timestamp,
            'full_text': text,
            'tokens': tokens,
            'token_count': len(tokens),
            'char_count': len(text),
            'filename': filename
        }
        
        documents_processed.inc()
        return doc
        
    except Exception as e:
        print(f"Tokenization failed for {url}: {e}")
        tokenization_errors.inc()
        return None


def process_batch(downloader: Downloader, ch, method, _properties, body):
    print("Received batch of size", len(body))
    batch_failed = False
    
    try:
        batch = json.loads(body)
        print(f"Processing batch with {len(batch)} items")
        
        for item in batch:
            try:
                # Download WARC file with error handling
                data = downloader.download_and_unzip(
                    item["metadata"]["filename"],
                    int(item["metadata"]["offset"]),
                    int(item["metadata"]["length"]),
                )
                warc_files_downloaded.inc()
                
                # Process WARC records with error handling
                try:
                    for record in WARCIterator(io.BytesIO(data)):
                        warc_records_total.inc()
                        if record.rec_type == "response":
                            warc_records_response.inc()
                            text_extractions_attempted.inc()
                            
                            try:
                                _text = trafilatura.extract(record.content_stream().read())
                                if _text and _text.strip():
                                    text_extractions_successful.inc()
                                    
                                    # Process and tokenize the text
                                    doc = process_and_tokenize_text(
                                        text=_text,
                                        url=item.get("surt_url", "unknown"),
                                        timestamp=item.get("timestamp", "unknown"),
                                        filename=item["metadata"]["filename"]
                                    )
                                    
                                    if doc:
                                        # Add to document batch
                                        document_batch.add_document(doc)
                                        
                                        # Check if batch should be uploaded
                                        if document_batch.should_upload():
                                            upload_documents_to_minio(document_batch.documents)
                                            document_batch.reset()
                                            
                            except Exception as e:
                                print(f"Text extraction failed for record: {e}")
                                # Continue processing other records
                                
                except Exception as e:
                    print(f"WARC parsing failed for {item['metadata']['filename']}: {e}")
                    # Continue with next item in batch
                    
            except Exception as e:
                error_type = type(e).__name__
                print(f"Download failed for {item['metadata']['filename']}: {error_type} - {e}")
                download_errors.labels(error_type=error_type).inc()
                items_skipped.inc()
                # Continue processing other items in batch
                
    except Exception as e:
        print(f"Batch processing failed: {e}")
        batch_errors.inc()
        batch_failed = True
    
    # Always acknowledge the message to prevent redelivery loops
    batch_counter.inc()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    
    if not batch_failed:
        print("Batch processed successfully")
    else:
        print("Batch completed with errors")


def main() -> None:
    global tokenizer, s3_client
    
    # Initialize components
    start_http_server(9002)
    print("🚀 Starting worker with tokenization and object storage...")
    
    # Load tokenizer
    print("📝 Loading tokenizer...")
    tokenizer = AutoTokenizer.from_pretrained('bert-base-uncased')
    print("✅ Tokenizer loaded")
    
    # Setup S3 client for MinIO
    print("🗄️ Connecting to MinIO...")
    os.environ['AWS_ACCESS_KEY_ID'] = 'admin'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'password123'
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        config=Config(signature_version='s3v4')
    )
    print("✅ MinIO connected")
    
    # Start processing
    downloader = CCDownloader(BASE_URL)
    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, ch, method, properties, body
        ),
    )
    
    print("🔄 Worker ready - waiting for batches...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
