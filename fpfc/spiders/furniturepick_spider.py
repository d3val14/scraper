import scrapy
from scrapy.spiders import SitemapSpider
from scrapy.http import Request
import csv
import os
from urllib.parse import urlparse
import logging

class FurniturepickSpider(SitemapSpider):
    name = "furniturepick"
    
    def __init__(self, *args, **kwargs):
        # Get environment variables or use defaults
        self.base_url = kwargs.get('base_url', os.environ.get('CURR_URL', 'https://www.furniturepick.com'))
        self.sitemap_offset = int(kwargs.get('offset', os.environ.get('SITEMAP_OFFSET', '0')))
        self.max_sitemaps = int(kwargs.get('max_sitemaps', os.environ.get('MAX_SITEMAPS', '2')))
        self.max_urls_per_sitemap = int(kwargs.get('max_urls_per_sitemap', os.environ.get('MAX_URLS_PER_SITEMAP', '100')))
        self.request_delay = float(kwargs.get('request_delay', os.environ.get('REQUEST_DELAY', '1.0')))
        
        # Set download delay
        self.download_delay = self.request_delay
        
        # Initialize sitemap URLs
        self.sitemap_urls = []
        
        # Output file
        self.chunk_num = self.sitemap_offset // int(os.environ.get('SITEMAPS_PER_JOB', '2'))
        self.output_file = f"products_chunk_{self.chunk_num:03d}.csv"
        
        # Initialize CSV file with headers
        self.init_csv()
        
        # Add print to verify initialization
        print(f"=== Spider initialized ===")
        print(f"Base URL: {self.base_url}")
        print(f"Output file: {self.output_file}")
        print(f"==========================")
        
        super().__init__(*args, **kwargs)
    
    def init_csv(self):
        """Initialize CSV file with headers"""
        if not os.path.exists(self.output_file):
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'url',
                    'product_name',
                    'price',
                    'sku',
                    'brand',
                    'availability',
                    'category',
                    'description',
                    'image_urls',
                    'timestamp'
                ])
            print(f"Created CSV file: {self.output_file} with headers")
    
    def start_requests(self):
        """Start by fetching robots.txt to find sitemaps"""
        print("\n=== start_requests() called ===")
        robots_url = f"{self.base_url}/robots.txt"
        print(f"Requesting robots.txt: {robots_url}")
        
        yield scrapy.Request(
            url=robots_url,
            callback=self.parse_robots,
            errback=self.handle_error,
            dont_filter=True,
            meta={
                'dont_retry': False,
                'handle_httpstatus_all': True
            }
        )
    
    def parse_robots(self, response):
        """Parse robots.txt to find sitemap URLs"""
        print(f"\n=== parse_robots() called ===")
        print(f"Response status: {response.status}")
        print(f"Response URL: {response.url}")
        print(f"Response length: {len(response.text)}")
        
        if response.status == 200:
            print("Response status 200, parsing content...")
            # Print first few lines of robots.txt for debugging
            lines = response.text.split('\n')[:10]
            print("First 10 lines of robots.txt:")
            for i, line in enumerate(lines):
                print(f"  {i+1}: {line}")
            
            # Extract sitemap URLs from robots.txt
            for line in response.text.split('\n'):
                if line.lower().startswith('sitemap:'):
                    sitemap_url = line.split(':', 1)[1].strip()
                    self.sitemap_urls.append(sitemap_url)
                    print(f"Found sitemap: {sitemap_url}")
                    self.logger.info(f"Found sitemap: {sitemap_url}")
        
        # If no sitemaps found in robots.txt or status not 200, try common locations
        if not self.sitemap_urls:
            print("\nNo sitemaps found in robots.txt, trying common locations...")
            common_sitemaps = [
                f"{self.base_url}/sitemap.xml",
                f"{self.base_url}/sitemap_index.xml",
                f"{self.base_url}/sitemap/sitemap.xml",
                f"{self.base_url}/sitemap/sitemap-index.xml",
                f"{self.base_url}/product-sitemap.xml",
                f"{self.base_url}/sitemap_products.xml",
            ]
            self.sitemap_urls.extend(common_sitemaps)
            print(f"Added {len(common_sitemaps)} common sitemap URLs")
        
        print(f"\nTotal sitemap URLs to process: {len(self.sitemap_urls)}")
        
        # Process sitemaps with offset and limit
        start_idx = self.sitemap_offset
        end_idx = min(start_idx + self.max_sitemaps, len(self.sitemap_urls))
        
        print(f"Processing sitemaps {start_idx} to {end_idx} (offset: {self.sitemap_offset}, limit: {self.max_sitemaps})")
        
        for i in range(start_idx, end_idx):
            sitemap_url = self.sitemap_urls[i]
            print(f"Requesting sitemap: {sitemap_url}")
            yield scrapy.Request(
                url=sitemap_url,
                callback=self.parse_sitemap_index,
                errback=self.handle_error,
                dont_filter=True,
                meta={
                    'sitemap_url': sitemap_url,
                    'handle_httpstatus_all': True
                }
            )
    
    def parse_sitemap_index(self, response):
        """Parse sitemap index file or individual sitemap"""
        print(f"\n=== parse_sitemap_index() called ===")
        print(f"Response status: {response.status}")
        print(f"Response URL: {response.url}")
        
        sitemap_url = response.meta.get('sitemap_url', response.url)
        
        if response.status != 200:
            print(f"Failed to fetch sitemap: {response.status}")
            return
        
        # Check if it's a sitemap index
        namespace = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # Try to parse as XML
        try:
            # Check for sitemap index
            sitemaps = response.xpath('//sm:sitemap | //sitemap')
            if sitemaps:
                print(f"Found sitemap index with {len(sitemaps)} entries")
                for sitemap in sitemaps:
                    loc = sitemap.xpath('.//sm:loc | .//loc').extract_first()
                    if loc:
                        print(f"  Found nested sitemap: {loc}")
                        yield scrapy.Request(
                            url=loc,
                            callback=self.parse_product_sitemap,
                            errback=self.handle_error,
                            dont_filter=True,
                            meta={
                                'sitemap_url': loc,
                                'handle_httpstatus_all': True
                            }
                        )
            else:
                # Treat as regular sitemap
                print("Treating as regular sitemap")
                yield from self.parse_product_sitemap(response)
                
        except Exception as e:
            print(f"Error parsing sitemap {sitemap_url}: {e}")
            self.logger.error(f"Error parsing sitemap {sitemap_url}: {e}")
    
    def parse_product_sitemap(self, response):
        """Parse product URLs from sitemap"""
        print(f"\n=== parse_product_sitemap() called ===")
        print(f"Response status: {response.status}")
        print(f"Response URL: {response.url}")
        
        if response.status != 200:
            print(f"Failed to fetch product sitemap: {response.status}")
            return
        
        namespace = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # Extract all URLs from sitemap
        urls = response.xpath('//sm:url | //url')
        print(f"Found {len(urls)} URL entries in sitemap")
        
        product_urls = []
        url_count = 0
        
        for url in urls:
            loc = url.xpath('.//sm:loc | .//loc').extract_first()
            if loc:
                # Filter for product URLs
                if any(keyword in loc.lower() for keyword in ['/product/', '/item/', '/p/', '/products/', '/shop/']):
                    product_urls.append(loc)
                    url_count += 1
                    print(f"  Found product URL: {loc}")
                    
                    if url_count >= self.max_urls_per_sitemap:
                        print(f"Reached max URLs per sitemap ({self.max_urls_per_sitemap})")
                        break
        
        print(f"Found {len(product_urls)} product URLs in {response.url}")
        
        # Save product URLs to CSV
        with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for url in product_urls:
                writer.writerow([
                    url,
                    '',  # product_name
                    '',  # price
                    '',  # sku
                    '',  # brand
                    '',  # availability
                    '',  # category
                    '',  # description
                    '',  # image_urls
                    '',  # timestamp
                ])
        
        print(f"Saved {len(product_urls)} URLs to {self.output_file}")
    
    def handle_error(self, failure):
        """Handle request errors"""
        print(f"\n=== Error Handler Called ===")
        print(f"Request URL: {failure.request.url}")
        print(f"Error: {repr(failure.value)}")
        self.logger.error(f"Request failed: {failure.request.url}")
        self.logger.error(f"Error: {repr(failure.value)}")