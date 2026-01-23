import json
import csv
import time
import random
import sys
from typing import Optional, Dict, List, Any
from datetime import datetime
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import re
import os
from io import StringIO

# Cloudflare bypass imports
import cloudscraper
import requests
from bs4 import BeautifulSoup

# ================= ENV =================

CURR_URL = os.getenv('CURR_URL', '').rstrip('/')
SITEMAP_OFFSET = int(os.getenv('SITEMAP_OFFSET', '0'))
MAX_SITEMAPS = int(os.getenv('MAX_SITEMAPS', '0'))
MAX_URLS_PER_SITEMAP = int(os.getenv('MAX_URLS_PER_SITEMAP', '0'))

SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml" if CURR_URL else ""
OUTPUT_CSV = f'products_chunk_{SITEMAP_OFFSET}.csv'

# ================= LOGGER =================

def log_msg(msg: str) -> None:
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}", file=sys.stderr, flush=True)

# ================= HTTP with Cloudflare Bypass =================

class CloudflareBypassSession:
    def __init__(self):
        self.session = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'linux',
                'mobile': False
            },
            delay=10
        )
        
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        })
        
    def get(self, url: str, retries: int = 5) -> Optional[str]:
        """Fetch URL with exponential backoff and better error handling."""
        for attempt in range(retries):
            try:
                log_msg(f"Fetching: {url} (attempt {attempt + 1}/{retries})")
                
                # Add delay between retries
                if attempt > 0:
                    wait_time = min(2 ** attempt + random.uniform(0, 1), 30)
                    log_msg(f"Waiting {wait_time:.1f}s before retry...")
                    time.sleep(wait_time)
                
                response = self.session.get(url, timeout=45, allow_redirects=True)
                
                # Log response details
                log_msg(f"Status: {response.status_code}, Content-Type: {response.headers.get('Content-Type', 'N/A')}")
                
                # Check for successful response
                response.raise_for_status()
                
                # Validate we got actual content, not a challenge page
                content_type = response.headers.get("Content-Type", "").lower()
                text_content = response.text
                
                # Check for Cloudflare challenge indicators
                if any(indicator in text_content.lower() for indicator in [
                    'checking your browser',
                    'enable javascript and cookies',
                    'cf-browser-verification',
                    'cloudflare',
                    'cf_chl_opt'
                ]):
                    log_msg("⚠️  Cloudflare challenge detected, retrying...")
                    continue
                
                # For XML content, verify it's actually XML
                if 'sitemap' in url.lower() or 'xml' in url.lower():
                    if not text_content.strip().startswith('<?xml') and not text_content.strip().startswith('<'):
                        log_msg(f"⚠️  Expected XML but got: {text_content[:200]}")
                        continue
                
                log_msg(f"✓ Successfully fetched {len(text_content)} bytes")
                return text_content

            except requests.exceptions.HTTPError as e:
                log_msg(f"❌ HTTP error {e.response.status_code}: {e}")
                if e.response.status_code == 403:
                    log_msg("⚠️  403 Forbidden - likely Cloudflare block")
                elif e.response.status_code == 503:
                    log_msg("⚠️  503 Service Unavailable - server may be rate limiting")
                    
            except requests.exceptions.Timeout:
                log_msg(f"❌ Timeout after 45s")
                
            except requests.exceptions.ConnectionError as e:
                log_msg(f"❌ Connection error: {e}")
                
            except Exception as e:
                log_msg(f"❌ Unexpected error: {type(e).__name__}: {e}")
        
        log_msg(f"❌ Failed to fetch after {retries} attempts")
        return None

    def get_json(self, url: str) -> Optional[Dict]:
        """Fetch and parse JSON content."""
        text = self.get(url)
        if text:
            try:
                data = json.loads(text)
                log_msg(f"✓ Successfully parsed JSON")
                return data
            except json.JSONDecodeError as e:
                log_msg(f"❌ JSON decode error at position {e.pos}: {e.msg}")
                log_msg(f"Content preview: {text[:500]}")
        return None

# Initialize session
session = CloudflareBypassSession()

def normalize_image(url: str) -> str:
    """Normalize image URL."""
    if not url:
        return ""
    if url.startswith('//'):
        return f'https:{url}'
    elif url.startswith('/'):
        return urljoin(CURR_URL, url)
    return url

def extract_json_from_script(html: str) -> Optional[Dict]:
    """Extract JSON data from script tags (fallback method)."""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Method 1: Look for product JSON in script tags
    script_patterns = [
        r'var\s+product\s*=\s*({.*?});',
        r'window\.product\s*=\s*({.*?});',
        r'Product\s*=\s*({.*?});',
        r'product: ({.*?}),',
        r'"product":({.*?}),',
        r'productData\s*=\s*({.*?});',
        r'item: ({.*?})'
    ]
    
    for script in soup.find_all('script'):
        if script.string:
            content = script.string.strip()
            for pattern in script_patterns:
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    try:
                        return json.loads(match.group(1))
                    except json.JSONDecodeError:
                        pass
    
    # Method 2: Look for JSON-LD data
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and '@type' in data:
                if data.get('@type') == 'Product':
                    return convert_ldjson_to_product(data)
        except:
            pass
    
    return None

def convert_ldjson_to_product(ld_data: Dict) -> Dict:
    """Convert JSON-LD to our product format."""
    product = {
        'id': ld_data.get('sku') or ld_data.get('productID') or '',
        'title': ld_data.get('name') or '',
        'vendor': ld_data.get('brand', {}).get('name') if isinstance(ld_data.get('brand'), dict) else ld_data.get('brand') or '',
        'type': ld_data.get('category') or '',
        'handle': '',
        'options': [],
        'variants': [],
        'featured_image': ld_data.get('image') or ''
    }
    
    # Handle offers
    offers = ld_data.get('offers', {})
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    
    variant = {
        'id': product['id'],
        'title': product['title'],
        'sku': product['id'],
        'barcode': '',
        'option1': '',
        'option2': '',
        'option3': '',
        'price': offers.get('price') if isinstance(offers, dict) else '',
        'available': offers.get('availability') == 'https://schema.org/InStock' if isinstance(offers, dict) else True
    }
    
    product['variants'].append(variant)
    return product

# ================= PRODUCT PROCESSING =================

def process_product(product_url: str, csv_writer, seen_urls: set) -> None:
    """Process a single product page."""
    if product_url in seen_urls:
        return
    seen_urls.add(product_url)
    
    log_msg(f"📦 Product: {product_url}")
    
    # Try JSON endpoint first (Shopify style)
    json_url = f"{product_url.rstrip('/')}.json"
    product_data = session.get_json(json_url)
    
    # If JSON endpoint fails, try scraping HTML
    if not product_data or 'product' not in product_data:
        log_msg(f"JSON endpoint failed, trying HTML scrape for: {product_url}")
        html = session.get(product_url)
        if html:
            product_data = extract_json_from_script(html)
            if product_data:
                product_data = {'product': product_data}
    
    if not product_data or 'product' not in product_data:
        log_msg("❌ Failed to extract product data")
        return
    
    product = product_data['product']
    
    if not product.get('variants'):
        log_msg("⚠️  No variants found")
        return
    
    log_msg(f"✓ Found {len(product['variants'])} variants")
    
    options = product.get('options', [])
    images = normalize_image(product.get('featured_image', ''))
    
    for variant in product['variants']:
        csv_writer.writerow([
            product.get('id', ''),
            product.get('title', '').strip(),
            product.get('vendor', '').strip(),
            product.get('product_type', product.get('type', '')).strip(),
            product.get('handle', '').strip(),
            variant.get('id', ''),
            variant.get('title', '').strip(),
            variant.get('sku', ''),
            variant.get('barcode', ''),
            options[0].get('name', '') if len(options) > 0 else '',
            variant.get('option1', ''),
            options[1].get('name', '') if len(options) > 1 else '',
            variant.get('option2', ''),
            options[2].get('name', '') if len(options) > 2 else '',
            variant.get('option3', ''),
            variant.get('price', ''),
            '1' if variant.get('available', False) else '0',
            f"{product_url.rstrip('/')}?variant={variant.get('id', '')}",
            images
        ])
    
    # Respectful delay - slightly longer for GitHub Actions
    time.sleep(0.3 + random.uniform(0, 0.2))

# ================= SITEMAP PARSING =================

def parse_sitemap(xml_content: str) -> List[str]:
    """Parse sitemap XML and extract URLs."""
    urls = []
    
    if not xml_content or not xml_content.strip():
        log_msg("❌ Empty sitemap content")
        return urls
    
    try:
        # Handle namespaces
        it = ET.iterparse(StringIO(xml_content))
        for _, el in it:
            el.tag = el.tag.split('}', 1)[-1]  # Remove namespace
        
        root = it.root
        
        # Find all URL elements
        for url in root.findall('.//url'):
            loc = url.find('loc')
            if loc is not None and loc.text:
                urls.append(loc.text)
        
        # Alternative: direct sitemap loc elements
        if not urls:
            for loc in root.findall('.//loc'):
                if loc.text:
                    urls.append(loc.text)
        
        log_msg(f"✓ Parsed {len(urls)} URLs from sitemap")
                    
    except Exception as e:
        log_msg(f"❌ Error parsing sitemap: {e}")
        log_msg(f"Content preview: {xml_content[:500]}")
    
    return urls

# ================= MAIN =================

def main():
    log_msg("=" * 60)
    log_msg("🚀 Scraper started")
    log_msg(f"Base URL: {CURR_URL}")
    log_msg(f"Sitemap offset: {SITEMAP_OFFSET}")
    log_msg(f"Max sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS else 'ALL'}")
    log_msg(f"Max URLs per sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP else 'ALL'}")
    log_msg("=" * 60)
    
    if not CURR_URL:
        log_msg("❌ CURR_URL environment variable not set!")
        sys.exit(1)
    
    # Load sitemap index
    log_msg(f"📍 Loading sitemap index: {SITEMAP_INDEX}")
    sitemap_index_content = session.get(SITEMAP_INDEX)
    
    if not sitemap_index_content:
        log_msg("❌ Failed to load sitemap index")
        sys.exit(1)
    
    # Parse sitemap index
    try:
        it = ET.iterparse(StringIO(sitemap_index_content))
        for _, el in it:
            el.tag = el.tag.split('}', 1)[-1]  # Remove namespace
        
        root = it.root
        sitemap_urls = []
        
        # Find all sitemap locations
        for sitemap in root.findall('.//sitemap'):
            loc = sitemap.find('loc')
            if loc is not None and loc.text:
                sitemap_urls.append(loc.text)
        
        # Alternative: direct loc elements
        if not sitemap_urls:
            for loc in root.findall('.//loc'):
                if loc.text:
                    sitemap_urls.append(loc.text)
        
        log_msg(f"✓ Found {len(sitemap_urls)} sitemaps in index")
        
    except Exception as e:
        log_msg(f"❌ Error parsing sitemap index: {e}")
        sys.exit(1)
    
    # Apply offset and limit
    start_idx = SITEMAP_OFFSET
    if MAX_SITEMAPS > 0:
        sitemap_urls = sitemap_urls[start_idx:start_idx + MAX_SITEMAPS]
    else:
        sitemap_urls = sitemap_urls[start_idx:]
    
    log_msg(f"📊 Sitemaps to process: {len(sitemap_urls)}")
    
    # Open CSV file
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        writer.writerow([
            'product_id', 'product_title', 'vendor', 'type', 'handle',
            'variant_id', 'variant_title', 'sku', 'barcode',
            'option_1_name', 'option_1_value',
            'option_2_name', 'option_2_value',
            'option_3_name', 'option_3_value',
            'variant_price', 'available', 'variant_url', 'image_url'
        ])
        
        seen_urls = set()
        total_products = 0
        
        for idx, sitemap_url in enumerate(sitemap_urls, 1):
            log_msg(f"\n{'='*60}")
            log_msg(f"📄 [{idx}/{len(sitemap_urls)}] Loading sitemap: {sitemap_url}")
            
            sitemap_content = session.get(sitemap_url)
            if not sitemap_content:
                log_msg("❌ Failed to load sitemap, skipping...")
                continue
            
            # Parse sitemap
            urls = parse_sitemap(sitemap_content)
            
            # Apply limit if specified
            if MAX_URLS_PER_SITEMAP > 0:
                urls = urls[:MAX_URLS_PER_SITEMAP]
            
            log_msg(f"📋 URLs to process: {len(urls)}")
            
            # Process each URL
            product_count = 0
            for url in urls:
                if '/products/' in url:  # Only process product pages
                    process_product(url, writer, seen_urls)
                    product_count += 1
                    total_products += 1
            
            log_msg(f"✓ Processed {product_count} products from this sitemap")
            
            # Clean up
            del sitemap_content
            import gc
            gc.collect()
    
    log_msg("=" * 60)
    log_msg(f"✅ Scraping completed!")
    log_msg(f"📊 Total products processed: {total_products}")
    log_msg(f"💾 Output file: {OUTPUT_CSV}")
    log_msg("=" * 60)

if __name__ == "__main__":
    main()