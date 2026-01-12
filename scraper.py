import requests
from bs4 import BeautifulSoup
import csv
import random
import time
import os
from ftplib import FTP_TLS

global_product_id = 1
LOCAL_INPUT = "input_urls.csv"
LOCAL_OUTPUT = "furniture_products.csv"


# -------------------- Helpers --------------------

def extract_text_or_none(element, default=None):
    return element.get_text(strip=True) if element else default


def extract_attr_or_none(element, attr, default=None):
    return element[attr] if element and element.has_attr(attr) else default


# -------------------- FTP --------------------

def get_ftp():
    ftp = FTP_TLS(timeout=30)
    ftp.connect(
        os.environ["FTP_HOST"],
        int(os.environ.get("FTP_PORT", 21))
    )
    ftp.login(
        os.environ["FTP_USER"],
        os.environ["FTP_PASSWORD"]
    )
    ftp.prot_p()  # secure data connection
    return ftp


def download_input_from_ftp():
    ftp = get_ftp()
    with open(LOCAL_INPUT, "wb") as f:
        ftp.retrbinary(
            f"RETR {os.environ['FTP_INPUT_PATH']}",
            f.write
        )
    ftp.quit()
    print("✅ Input CSV downloaded from FTP")


def upload_output_to_ftp():
    ftp = get_ftp()
    with open(LOCAL_OUTPUT, "rb") as f:
        ftp.storbinary(
            f"STOR {os.environ['FTP_OUTPUT_PATH']}",
            f
        )
    ftp.quit()
    print("✅ Output CSV uploaded to FTP")


# -------------------- Scraper --------------------

def scrape_product(url):
    global global_product_id

    response = requests.get(url, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    product_data = []

    name = extract_text_or_none(
        soup.find("div", class_="product-name")
        .find("h1", {"itemprop": "name"})
    )
    sku = extract_attr_or_none(
        soup.find("meta", {"itemprop": "sku"}),
        "content"
    )
    brand = extract_text_or_none(
        soup.select_one("p.manufacturer a:nth-of-type(2)")
    )
    collection = extract_text_or_none(
        soup.select_one("p.manufacturer a:nth-of-type(1)")
    )
    image = extract_attr_or_none(
        soup.find("meta", {"itemprop": "image"}),
        "content"
    )

    price_el = soup.select_one(".price-box .price")
    price = extract_text_or_none(price_el, "N/A").replace("$", "")

    main_id = global_product_id

    product_data.append({
        "product_id": global_product_id,
        "main_product_id": main_id,
        "product_type": "simple",
        "url": url,
        "name": name,
        "sku": sku,
        "brand": brand,
        "collection": collection,
        "image": image,
        "price": price
    })

    global_product_id += 1
    return product_data


# -------------------- Main --------------------

def main():
    download_input_from_ftp()

    with open(LOCAL_INPUT) as f:
        urls = [row[0] for row in csv.reader(f) if row]

    columns = [
        "product_id", "main_product_id", "product_type",
        "url", "name", "sku", "brand",
        "collection", "image", "price"
    ]

    with open(LOCAL_OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()

        for i, url in enumerate(urls, start=1):
            print(f"[{i}] Scraping {url}")
            for row in scrape_product(url):
                writer.writerow(row)

            if i % random.randint(30, 50) == 0:
                time.sleep(random.uniform(3, 7))

    upload_output_to_ftp()


if __name__ == "__main__":
    main()