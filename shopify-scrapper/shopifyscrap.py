import json
import csv
import time
import random
import sys
import os
import re
import gc
from typing import Optional, Dict, List
from datetime import datetime
from urllib.parse import urljoin
from io import StringIO

import requests
import cloudscraper
import xml.etree.ElementTree as ET
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
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)

# ================= HTTP =================

def is_cloudflare_response(resp: requests.Response) -> bool:
    h = {k.lower(): v.lower() for k, v in resp.headers.items()}
    return (
        "cloudflare" in h.get("server", "")
        or "cf-ray" in h
        or resp.status_code in (403, 429)
        or "attention required" in resp.text.lower()
    )

class SmartSession:
    def __init__(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }

        self.normal = requests.Session()
        self.normal.headers.update(headers)

        self.cf = cloudscraper.create_scraper(
            browser={"browser": "firefox", "platform": "windows", "mobile": False},
            delay=8
        )
        self.cf.headers.update(headers)

    def get(self, url: str, retries: int = 3) -> Optional[str]:
        for attempt in range(retries):
            try:
                log_msg(f"GET(normal): {url}")
                r = self.normal.get(url, timeout=25)
                r.raise_for_status()

                if is_cloudflare_response(r):
                    raise RuntimeError("Cloudflare detected")

                return r.text.strip()

            except Exception:
                try:
                    log_msg(f"GET(cf): {url}")
                    r = self.cf.get(url, timeout=30)
                    r.raise_for_status()
                    return r.text.strip()
                except Exception as e:
                    log_msg(f"HTTP error: {e}")
                    time.sleep(2 ** attempt)
        return None

    def get_json(self, url: str) -> Optional[Dict]:
        text = self.get(url)
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None

session = SmartSession()

# ================= HELPERS =================

def normalize_image(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        return f"https:{url}"
    if url.startswith("/"):
        return urljoin(CURR_URL, url)
    return url

# ================= HTML FALLBACK =================

def extract_json_from_script(html: str) -> Optional[Dict]:
    soup = BeautifulSoup(html, "html.parser")

    patterns = [
        r'"product":\s*({.*?})\s*,\s*"collections"',
        r'window\.product\s*=\s*({.*?});',
    ]

    for s in soup.find_all("script"):
        if not s.string:
            continue
        txt = s.string
        for p in patterns:
            m = re.search(p, txt, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(1))
                except json.JSONDecodeError:
                    pass

    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string)
            if isinstance(data, dict) and data.get("@type") == "Product":
                return {
                    "id": data.get("sku", ""),
                    "title": data.get("name", ""),
                    "vendor": data.get("brand", {}).get("name", ""),
                    "variants": [{
                        "id": data.get("sku", ""),
                        "title": data.get("name", ""),
                        "sku": data.get("sku", ""),
                        "price": data.get("offers", {}).get("price", ""),
                        "available": True,
                    }],
                    "featured_image": data.get("image", "")
                }
        except Exception:
            pass

    return None

# ================= PRODUCT =================

def process_product(url: str, writer, seen: set):
    if url in seen:
        return
    seen.add(url)

    log_msg(f"Product: {url}")

    data = session.get_json(f"{url.rstrip('/')}.json")

    if not data or "product" not in data:
        html = session.get(url)
        if not html:
            return
        prod = extract_json_from_script(html)
        if not prod:
            return
        data = {"product": prod}

    product = data["product"]
    variants = product.get("variants", [])
    if not variants:
        return

    options = product.get("options", [])
    image = normalize_image(product.get("featured_image", ""))

    for v in variants:
        writer.writerow([
            product.get("id", ""),
            product.get("title", ""),
            product.get("vendor", ""),
            product.get("product_type", ""),
            product.get("handle", ""),
            v.get("id", ""),
            v.get("title", ""),
            v.get("sku", ""),
            v.get("barcode", ""),
            options[0]["name"] if len(options) > 0 else "",
            v.get("option1", ""),
            options[1]["name"] if len(options) > 1 else "",
            v.get("option2", ""),
            options[2]["name"] if len(options) > 2 else "",
            v.get("option3", ""),
            v.get("price", ""),
            "1" if v.get("available") else "0",
            f"{url}?variant={v.get('id')}",
            image
        ])

    time.sleep(0.15 + random.random() * 0.1)

# ================= SITEMAP =================

def parse_sitemap(xml: str) -> List[str]:
    urls = []
    try:
        xml = xml.strip()
        it = ET.iterparse(StringIO(xml))
        for _, el in it:
            el.tag = el.tag.split("}", 1)[-1]
        root = it.root
        for loc in root.findall(".//loc"):
            if loc.text:
                urls.append(loc.text.strip())
    except Exception as e:
        log_msg(f"Sitemap parse error: {e}")
    return urls

# ================= MAIN =================

def main():
    log_msg("Scraper started")

    index_xml = session.get(SITEMAP_INDEX)
    if not index_xml:
        sys.exit(1)

    sitemap_urls = parse_sitemap(index_xml)
    sitemap_urls = sitemap_urls[SITEMAP_OFFSET:]
    if MAX_SITEMAPS:
        sitemap_urls = sitemap_urls[:MAX_SITEMAPS]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            'product_id','product_title','vendor','type','handle',
            'variant_id','variant_title','sku','barcode',
            'option_1_name','option_1_value',
            'option_2_name','option_2_value',
            'option_3_name','option_3_value',
            'variant_price','available','variant_url','image_url'
        ])

        seen = set()

        for sm in sitemap_urls:
            log_msg(f"Sitemap: {sm}")
            xml = session.get(sm)
            if not xml:
                continue

            urls = parse_sitemap(xml)
            if MAX_URLS_PER_SITEMAP:
                urls = urls[:MAX_URLS_PER_SITEMAP]

            for u in urls:
                if "/products/" in u:
                    process_product(u, writer, seen)

            gc.collect()

    log_msg(f"Done: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()