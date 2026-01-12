import os
import csv
import time
import random
import socket
import requests
from ftplib import FTP, FTP_TLS, error_perm
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------
# ENV
# --------------------------------------------------
load_dotenv()

LOCAL_INPUT = "input_urls.csv"
LOCAL_OUTPUT = "furniture_products.csv"
global_product_id = 1

# --------------------------------------------------
# HTTP SESSION (ANTI-403)
# --------------------------------------------------
def create_session():
    session = requests.Session()

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.furniturepick.com/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })

    retries = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


SESSION = create_session()

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def extract_text_or_none(element, default=None):
    return element.get_text(strip=True) if element else default


def extract_attr_or_none(element, attr, default=None):
    return element[attr] if element and element.has_attr(attr) else default


# --------------------------------------------------
# FTP (PLAIN FIRST, FTPS FALLBACK)
# --------------------------------------------------
def get_ftp():
    host = os.getenv("FTP_HOST")
    port = int(os.getenv("FTP_PORT", 21))
    user = os.getenv("FTP_USER")
    password = os.getenv("FTP_PASSWORD")

    if not all([host, user, password]):
        raise RuntimeError("❌ Missing FTP environment variables")

    socket.setdefaulttimeout(30)

    # ---- Plain FTP (most shared hosts) ----
    try:
        ftp = FTP()
        ftp.connect(host, port)
        ftp.set_pasv(True)
        ftp.login(user, password)
        print("✅ Connected using plain FTP")
        return ftp
    except error_perm as e:
        print(f"⚠️ Plain FTP failed: {e}")

    # ---- FTPS fallback ----
    try:
        ftp = FTP_TLS()
        ftp.connect(host, port)
        ftp.login(user, password)
        ftp.prot_p()
        ftp.set_pasv(True)
        print("✅ Connected using FTPS")
        return ftp
    except error_perm as e:
        raise RuntimeError(f"❌ FTP login failed: {e}")


def download_input_from_ftp():
    ftp = get_ftp()
    with open(LOCAL_INPUT, "wb") as f:
        ftp.retrbinary(f"RETR {os.getenv('FTP_INPUT_PATH')}", f.write)
    ftp.quit()
    print("✅ Input CSV downloaded")


def upload_output_to_ftp():
    ftp = get_ftp()
    with open(LOCAL_OUTPUT, "rb") as f:
        ftp.storbinary(f"STOR {os.getenv('FTP_OUTPUT_PATH')}", f)
    ftp.quit()
    print("✅ Output CSV uploaded")


# --------------------------------------------------
# SCRAPER
# --------------------------------------------------
def scrape_product(url):
    global global_product_id

    try:
        response = SESSION.get(url, timeout=30)

        if response.status_code == 403:
            print(f"⛔ 403 blocked → skipped: {url}")
            return []

        if not response.ok:
            print(f"⚠️ HTTP {response.status_code} → skipped: {url}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")

        name = extract_text_or_none(
            soup.find("div", class_="product-name")
            .find("h1", {"itemprop": "name"})
        )
        sku = extract_attr_or_none(
            soup.find("meta", {"itemprop": "sku"}), "content"
        )
        brand = extract_text_or_none(
            soup.select_one("p.manufacturer a:nth-of-type(2)")
        )
        collection = extract_text_or_none(
            soup.select_one("p.manufacturer a:nth-of-type(1)")
        )
        image = extract_attr_or_none(
            soup.find("meta", {"itemprop": "image"}), "content"
        )

        price_el = soup.select_one(".price-box .price")
        price = extract_text_or_none(price_el, "N/A").replace("$", "")

        row = {
            "product_id": global_product_id,
            "main_product_id": global_product_id,
            "product_type": "simple",
            "url": url,
            "name": name,
            "sku": sku,
            "brand": brand,
            "collection": collection,
            "image": image,
            "price": price,
        }

        global_product_id += 1
        return [row]

    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return []


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    download_input_from_ftp()

    with open(LOCAL_INPUT) as f:
        urls = [row[0] for row in csv.reader(f) if row]

    columns = [
        "product_id",
        "main_product_id",
        "product_type",
        "url",
        "name",
        "sku",
        "brand",
        "collection",
        "image",
        "price",
    ]

    with open(LOCAL_OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()

        for i, url in enumerate(urls, start=1):
            print(f"[{i}/{len(urls)}] Scraping")
            rows = scrape_product(url)
            for row in rows:
                writer.writerow(row)

            time.sleep(random.uniform(1.2, 2.5))  # anti-ban

    upload_output_to_ftp()


if __name__ == "__main__":
    main()