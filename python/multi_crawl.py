"""
Multi-crawl processing module with URL deduplication.

This module provides functionality to process multiple Common Crawl versions
sequentially while ensuring each URL is only processed once across all crawls.
"""

import json
from typing import Set, Dict, List, Optional
from dataclasses import dataclass
from pathlib import Path
import requests


@dataclass
class CrawlCheckpoint:
    """Checkpoint data for a single crawl."""
    crawl_version: str
    cluster_idx_filename: str
    last_processed_line: int
    total_batches_sent: int
    total_lines: int
    status: str  # 'pending', 'in_progress', 'completed'
    start_time: Optional[str] = None
    last_update: Optional[str] = None


class URLDeduplicator:
    """Handles URL deduplication across multiple crawls."""
    
    def __init__(self):
        self._seen_urls: Set[str] = set()
        self._total_urls_processed = 0
        self._duplicates_skipped = 0
    
    def normalize_url(self, surt_url: str) -> str:
        """
        Normalize URL for deduplication.
        
        Args:
            surt_url: SURT format URL with timestamp
            
        Returns:
            Normalized URL without timestamp
        """
        # Remove timestamp from SURT URL
        # Example: "com,example)/page 20240722120756" -> "com,example)/page"
        return surt_url.split()[0] if ' ' in surt_url else surt_url
    
    def should_process_url(self, surt_url: str) -> bool:
        """
        Check if URL should be processed based on deduplication rules.
        
        Args:
            surt_url: SURT format URL to check
            
        Returns:
            True if URL should be processed, False if it's a duplicate
        """
        normalized_url = self.normalize_url(surt_url)
        self._total_urls_processed += 1
        
        if normalized_url in self._seen_urls:
            self._duplicates_skipped += 1
            return False
        
        self._seen_urls.add(normalized_url)
        return True
    
    def get_stats(self) -> Dict[str, int]:
        """Get deduplication statistics."""
        return {
            'total_urls_processed': self._total_urls_processed,
            'unique_urls': len(self._seen_urls),
            'duplicates_skipped': self._duplicates_skipped
        }


class CrawlCheckpointManager:
    """Manages checkpoints for individual crawls."""
    
    def __init__(self, checkpoint_dir: str = "."):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)
    
    def _get_checkpoint_filename(self, crawl_version: str) -> Path:
        """Get checkpoint filename for a crawl version."""
        safe_name = crawl_version.replace('-', '_').replace('.', '_')
        return self.checkpoint_dir / f"checkpoint_{safe_name}.json"
    
    def load_checkpoint(self, crawl_version: str) -> CrawlCheckpoint:
        """
        Load checkpoint for a crawl version.
        
        Args:
            crawl_version: Common Crawl version (e.g., 'CC-MAIN-2024-30')
            
        Returns:
            CrawlCheckpoint with loaded or default data
        """
        checkpoint_file = self._get_checkpoint_filename(crawl_version)
        
        try:
            with open(checkpoint_file, 'r') as f:
                data = json.load(f)
                return CrawlCheckpoint(**data)
        except FileNotFoundError:
            # Return default checkpoint for new crawl
            return CrawlCheckpoint(
                crawl_version=crawl_version,
                cluster_idx_filename="",
                last_processed_line=0,
                total_batches_sent=0,
                total_lines=0,
                status='pending'
            )
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Warning: Corrupted checkpoint for {crawl_version}: {e}")
            return CrawlCheckpoint(
                crawl_version=crawl_version,
                cluster_idx_filename="",
                last_processed_line=0,
                total_batches_sent=0,
                total_lines=0,
                status='pending'
            )
    
    def save_checkpoint(self, checkpoint: CrawlCheckpoint) -> None:
        """
        Save checkpoint for a crawl version.
        
        Args:
            checkpoint: CrawlCheckpoint to save
        """
        checkpoint_file = self._get_checkpoint_filename(checkpoint.crawl_version)
        
        # Convert dataclass to dict for JSON serialization
        checkpoint_data = {
            'crawl_version': checkpoint.crawl_version,
            'cluster_idx_filename': checkpoint.cluster_idx_filename,
            'last_processed_line': checkpoint.last_processed_line,
            'total_batches_sent': checkpoint.total_batches_sent,
            'total_lines': checkpoint.total_lines,
            'status': checkpoint.status,
            'start_time': checkpoint.start_time,
            'last_update': checkpoint.last_update
        }
        
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
        except IOError as e:
            print(f"Error saving checkpoint for {checkpoint.crawl_version}: {e}")
    
    def is_crawl_completed(self, crawl_version: str) -> bool:
        """Check if a crawl is completed."""
        checkpoint = self.load_checkpoint(crawl_version)
        return checkpoint.status == 'completed'
    
    def list_crawl_statuses(self, crawl_versions: List[str]) -> Dict[str, str]:
        """Get status of multiple crawls."""
        statuses = {}
        for crawl_version in crawl_versions:
            checkpoint = self.load_checkpoint(crawl_version)
            statuses[crawl_version] = checkpoint.status
        return statuses


class ClusterIndexDownloader:
    """Downloads cluster.idx files for crawl versions."""
    
    def __init__(self, base_url: str = "https://data.commoncrawl.org"):
        self.base_url = base_url
    
    def get_cluster_idx_url(self, crawl_version: str) -> str:
        """Get the URL for a crawl version's cluster.idx file."""
        return f"{self.base_url}/cc-index/collections/{crawl_version}/indexes/cluster.idx"
    
    def get_local_cluster_idx_path(self, crawl_version: str) -> Path:
        """Get local path for cluster.idx file."""
        safe_name = crawl_version.replace('-', '_').replace('.', '_')
        return Path(f"cluster_{safe_name}.idx")
    
    def download_cluster_idx(self, crawl_version: str, force_download: bool = False) -> Path:
        """
        Download cluster.idx file for a crawl version.
        
        Args:
            crawl_version: Common Crawl version
            force_download: Force re-download even if file exists
            
        Returns:
            Path to downloaded cluster.idx file
            
        Raises:
            requests.RequestException: If download fails
        """
        local_path = self.get_local_cluster_idx_path(crawl_version)
        
        # Skip download if file already exists and not forcing
        if local_path.exists() and not force_download:
            print(f"Using existing cluster.idx for {crawl_version}: {local_path}")
            return local_path
        
        url = self.get_cluster_idx_url(crawl_version)
        print(f"Downloading cluster.idx for {crawl_version} from {url}...")
        
        try:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Download with progress indication
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Show progress every 10MB
                        if downloaded % (10 * 1024 * 1024) == 0:
                            if total_size > 0:
                                progress = (downloaded / total_size) * 100
                                print(f"Download progress: {progress:.1f}% ({downloaded:,} bytes)")
            
            print(f"✅ Downloaded cluster.idx for {crawl_version}: {local_path}")
            return local_path
            
        except requests.RequestException as e:
            print(f"❌ Failed to download cluster.idx for {crawl_version}: {e}")
            # Clean up partial download
            if local_path.exists():
                local_path.unlink()
            raise


def parse_crawl_versions(crawl_versions_str: str) -> List[str]:
    """
    Parse crawl versions from command line argument.
    
    Args:
        crawl_versions_str: Comma-separated crawl versions
        
    Returns:
        List of crawl version strings
    """
    versions = [v.strip() for v in crawl_versions_str.split(',')]
    
    # Validate crawl version format
    valid_versions = []
    for version in versions:
        if not version:
            continue
        
        # Basic validation - should start with CC-MAIN-
        if not version.startswith('CC-MAIN-'):
            print(f"Warning: Invalid crawl version format: {version}")
            continue
        
        valid_versions.append(version)
    
    if not valid_versions:
        raise ValueError("No valid crawl versions provided")
    
    return valid_versions