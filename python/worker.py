import io
import json
import os
import time
import pandas as pd
import boto3
from botocore.client import Config
from prometheus_client import start_http_server, Gauge, Counter
import trafilatura
from transformers import AutoTokenizer
from warcio.archiveiterator import WARCIterator

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

# Data volume and processing metrics
bytes_downloaded = Counter("worker_bytes_downloaded", "Total bytes downloaded from WARC files")
batch_processing_time = Gauge("worker_batch_processing_seconds", "Time taken to process each batch")
batch_size_items = Gauge("worker_batch_size_items", "Number of items in current batch")
document_batch_size = Gauge("worker_document_batch_size", "Number of documents in current upload batch")
document_batch_size_bytes = Gauge("worker_document_batch_size_bytes", "Size of current document batch in bytes")
documents_filtered_length = Counter("worker_documents_filtered_length", "Documents filtered out by length constraints")
upload_attempts = Counter("worker_upload_attempts", "Number of upload attempts")
upload_failures = Counter("worker_upload_failures", "Number of upload failures")


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
document_batch = DocumentBatch(max_docs=10, max_size_mb=1, max_time_minutes=1)  # Production batch size
s3_client = None
s3_bucket_name = 'processed-docs'


def upload_documents_to_minio(documents, bucket_name=None):
    """Upload documents to MinIO as Parquet file"""
    global s3_client, s3_bucket_name
    if not s3_client:
        print("❌ Upload skipped: S3 client not initialized")
        return
    
    if not documents:
        print("❌ Upload skipped: No documents to upload")
        return
    
    if bucket_name is None:
        bucket_name = s3_bucket_name
    
    print(f"🔄 Uploading {len(documents)} documents to {bucket_name}...")
    upload_attempts.inc()
    
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
        upload_failures.inc()
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
    batch_start_time = time.time()
    
    try:
        batch = json.loads(body)
        print(f"Processing batch with {len(batch)} items")
        batch_size_items.set(len(batch))
        
        for item in batch:
            try:
                # Download WARC file with error handling
                data = downloader.download_and_unzip(
                    item["metadata"]["filename"],
                    int(item["metadata"]["offset"]),
                    int(item["metadata"]["length"]),
                )
                warc_files_downloaded.inc()
                bytes_downloaded.inc(len(data))
                
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
                                    # Apply document length filter (500-1,000,000 characters)
                                    text_length = len(_text)
                                    if text_length < 100 or text_length > 1_000_000:
                                        documents_filtered_length.inc()
                                        continue
                                    
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
                                        
                                        # Update metrics for current document batch
                                        document_batch_size.set(len(document_batch.documents))
                                        document_batch_size_bytes.set(document_batch.total_size)
                                        
                                        # Check if batch should be uploaded
                                        if document_batch.should_upload():
                                            print(f"📦 Batch threshold reached: {len(document_batch.documents)} docs, {document_batch.total_size/1024:.1f}KB, {(time.time()-document_batch.start_time)/60:.1f}min")
                                            upload_documents_to_minio(document_batch.documents)
                                            document_batch.reset()
                                            document_batch_size.set(0)
                                            document_batch_size_bytes.set(0)
                                            
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
    
    # Record batch processing time
    batch_processing_time.set(time.time() - batch_start_time)
    
    # Always acknowledge the message to prevent redelivery loops
    batch_counter.inc()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    
    if not batch_failed:
        print("Batch processed successfully")
    else:
        print("Batch completed with errors")


def main() -> None:
    global tokenizer, s3_client
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Common Crawl Worker')
    parser.add_argument('--s3-endpoint', default='http://localhost:9010', 
                       help='S3 endpoint URL (default: http://localhost:9010)')
    parser.add_argument('--s3-access-key', default='admin',
                       help='S3 access key (default: admin)')
    parser.add_argument('--s3-secret-key', default='password123',
                       help='S3 secret key (default: password123)')
    parser.add_argument('--s3-bucket', default='processed-docs',
                       help='S3 bucket name (default: processed-docs)')
    parser.add_argument('--tokenizer-model', default='bert-base-uncased',
                       help='HuggingFace tokenizer model (default: bert-base-uncased)')
    parser.add_argument('--prometheus-port', type=int, default=9001,
                       help='Prometheus metrics port (default: 9001)')
    parser.add_argument('--batch-max-docs', type=int, default=10,
                       help='Maximum documents per batch (default: 10)')
    parser.add_argument('--batch-max-size-mb', type=int, default=1,
                       help='Maximum batch size in MB (default: 1)')
    parser.add_argument('--batch-max-time-minutes', type=int, default=1,
                       help='Maximum batch time in minutes (default: 1)')
    
    args = parser.parse_args()
    
    # Initialize components
    start_http_server(args.prometheus_port)
    print(f"🚀 Starting worker with tokenization and object storage on port {args.prometheus_port}...")
    
    # Load tokenizer
    print(f"📝 Loading tokenizer ({args.tokenizer_model})...")
    tokenizer = AutoTokenizer.from_pretrained(args.tokenizer_model)
    print("✅ Tokenizer loaded")
    
    # Setup S3 client for MinIO
    print(f"🗄️ Connecting to object store ({args.s3_endpoint})...")
    os.environ['AWS_ACCESS_KEY_ID'] = args.s3_access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = args.s3_secret_key
    s3_client = boto3.client(
        's3',
        endpoint_url=args.s3_endpoint,
        config=Config(signature_version='s3v4')
    )
    print("✅ Object store connected")
    
    # Update global document batch and bucket name with CLI args
    global document_batch, s3_bucket_name
    document_batch = DocumentBatch(
        max_docs=args.batch_max_docs, 
        max_size_mb=args.batch_max_size_mb, 
        max_time_minutes=args.batch_max_time_minutes
    )
    s3_bucket_name = args.s3_bucket
    
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
