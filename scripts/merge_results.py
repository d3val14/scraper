#!/usr/bin/env python3
import os
import sys
import pandas as pd
from datetime import datetime


CHUNKS_DIR = os.getenv("CHUNKS_DIR", "chunks")
OUTPUT_PREFIX_PRODUCTS = "merged_products"
OUTPUT_PREFIX_SELLERS = "merged_sellers"


def collect_csv_files(base_dir: str):
    product_files = []
    seller_files = []

    for root, _, files in os.walk(base_dir):
        for filename in files:
            if not filename.endswith(".csv"):
                continue

            path = os.path.join(root, filename)

            lname = filename.lower()
            if "product" in lname:
                product_files.append(path)
            elif "seller" in lname:
                seller_files.append(path)

    return product_files, seller_files


def merge_csv(files, sort_cols, output_prefix):
    if not files:
        print(f"[INFO] No files found for {output_prefix}")
        return None

    print(f"[INFO] Merging {len(files)} files for {output_prefix}")

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            dfs.append(df)
            print(f"  ✓ {f} ({len(df)} rows)")
        except Exception as e:
            print(f"  ✗ Skipping {f}: {e}")

    if not dfs:
        print(f"[WARN] No valid CSVs for {output_prefix}")
        return None

    merged = pd.concat(dfs, ignore_index=True)

    for col in sort_cols:
        if col not in merged.columns:
            raise ValueError(
                f"Missing required column '{col}' in {output_prefix}"
            )

    merged.sort_values(sort_cols, inplace=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{output_prefix}_{ts}.csv"
    merged.to_csv(output_file, index=False)

    print(f"[OK] Created {output_file} ({len(merged)} rows)")
    return output_file


def main():
    if not os.path.isdir(CHUNKS_DIR):
        print(f"[ERROR] Directory not found: {CHUNKS_DIR}")
        sys.exit(1)

    product_files, seller_files = collect_csv_files(CHUNKS_DIR)

    merge_csv(
        files=product_files,
        sort_cols=["product_id"],
        output_prefix=OUTPUT_PREFIX_PRODUCTS,
    )

    merge_csv(
        files=seller_files,
        sort_cols=["product_id", "seller"],
        output_prefix=OUTPUT_PREFIX_SELLERS,
    )


if __name__ == "__main__":
    main()