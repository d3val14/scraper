#!/usr/bin/env python3
"""
FP FC Scraper - Main entry point for GitHub Actions workflow
"""

import os
import sys
import subprocess
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more output
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_dependencies():
    """Check if required packages are installed"""
    try:
        import scrapy
        from scrapy.crawler import CrawlerProcess
        logger.info("✓ Scrapy is installed")
        return True
    except ImportError as e:
        logger.error(f"Missing dependency: {e}")
        logger.info("Installing required packages...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", 
                             "scrapy==2.8.0", "lxml==4.9.2", "cssselect==1.2.0", "requests==2.28.2"])
        return True

def run_spider():
    """Run the Scrapy spider with environment configuration"""
    
    # Get environment variables
    base_url = os.environ.get('CURR_URL', 'https://www.furniturepick.com')
    offset = os.environ.get('SITEMAP_OFFSET', '0')
    max_sitemaps = os.environ.get('MAX_SITEMAPS', '2')
    max_urls = os.environ.get('MAX_URLS_PER_SITEMAP', '100')
    max_workers = os.environ.get('MAX_WORKERS', '4')
    delay = os.environ.get('REQUEST_DELAY', '1.0')
    sitemaps_per_job = os.environ.get('SITEMAPS_PER_JOB', '2')
    
    logger.info("=" * 60)
    logger.info("FP FC Scraper Started")
    logger.info("=" * 60)
    logger.info(f"Base URL: {base_url}")
    logger.info(f"Sitemap Offset: {offset}")
    logger.info(f"Max Sitemaps: {max_sitemaps}")
    logger.info(f"Max URLs per Sitemap: {max_urls}")
    logger.info(f"Max Workers: {max_workers}")
    logger.info(f"Request Delay: {delay}s")
    logger.info(f"Sitemaps per job: {sitemaps_per_job}")
    logger.info("=" * 60)
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    spider_path = os.path.join(script_dir, "spiders", "furniturepick_spider.py")
    
    logger.info(f"Spider path: {spider_path}")
    
    if not os.path.exists(spider_path):
        logger.error(f"Spider file not found: {spider_path}")
        return 1
    
    # Create scrapy command with full debug output
    cmd = [
        "scrapy", "runspider",
        spider_path,
        "-a", f"base_url={base_url}",
        "-a", f"offset={offset}",
        "-a", f"max_sitemaps={max_sitemaps}",
        "-a", f"max_urls_per_sitemap={max_urls}",
        "-a", f"request_delay={delay}",
        "-s", "LOG_LEVEL=DEBUG",  # Changed to DEBUG
        "-s", f"CONCURRENT_REQUESTS={max_workers}",
        "-s", f"DOWNLOAD_DELAY={delay}",
        "-s", "USER_AGENT=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "-s", "ROBOTSTXT_OBEY=False",
        "-s", "HTTPCACHE_ENABLED=False",  # Disable cache for debugging
        "-s", "DOWNLOAD_TIMEOUT=30",
        "-s", "RETRY_ENABLED=True",
        "-s", "RETRY_TIMES=2",
        "--nolog",  # Reduce Scrapy's internal logging
    ]
    
    logger.info(f"Running command: {' '.join(cmd)}")
    
    # Run spider
    try:
        # Run with output capture to see prints
        result = subprocess.run(cmd, check=False, capture_output=True, text=True)
        
        # Print stdout and stderr
        if result.stdout:
            print("\n=== Spider Output ===")
            print(result.stdout)
            print("=====================\n")
        
        if result.stderr:
            print("\n=== Spider Errors ===")
            print(result.stderr)
            print("====================\n")
        
        if result.returncode == 0:
            logger.info("Spider completed successfully")
        else:
            logger.error(f"Spider failed with exit code {result.returncode}")
            return result.returncode
        
        # List generated CSV files
        csv_files = [f for f in os.listdir('.') if f.startswith('products_chunk_') and f.endswith('.csv')]
        csv_files.extend([f for f in os.listdir(script_dir) if f.startswith('products_chunk_') and f.endswith('.csv')])
        
        if csv_files:
            logger.info(f"Generated CSV files: {csv_files}")
            
            # Count total URLs
            total_urls = 0
            for csv_file in set(csv_files):  # Use set to avoid duplicates
                try:
                    file_path = csv_file if os.path.exists(csv_file) else os.path.join(script_dir, csv_file)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        urls = sum(1 for line in f) - 1  # Subtract header
                        total_urls += urls
                    logger.info(f"  {csv_file}: {urls} product URLs")
                except Exception as e:
                    logger.error(f"Error reading {csv_file}: {e}")
            
            logger.info(f"Total product URLs found: {total_urls}")
        else:
            logger.warning("No CSV files were generated")
            # List all files in current directory for debugging
            logger.info("Files in current directory:")
            for f in os.listdir('.'):
                logger.info(f"  {f}")
            logger.info(f"Files in {script_dir}:")
            for f in os.listdir(script_dir):
                logger.info(f"  {f}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1

def main():
    """Main entry point"""
    try:
        # Check and install dependencies
        if not check_dependencies():
            logger.error("Failed to install dependencies")
            return 1
        
        # Run the spider
        return run_spider()
        
    except KeyboardInterrupt:
        logger.info("Scraper interrupted by user")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())