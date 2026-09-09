"""Downloads the real UCI Online Retail II dataset and splits it into the
3 raw CSVs this pipeline ingests. Cleaning decisions (cancellations, missing
customer ids, invalid prices) are left to the Spark jobs, not made here,
this script only combines and reshapes, it doesn't clean.

Usage: python scripts/fetch_data.py
Writes to data/raw/ (gitignored, run this once before using the pipeline).
"""

from pathlib import Path
from urllib.request import urlretrieve
import zipfile

import pandas as pd

SOURCE_URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
CACHE_DIR = ROOT / ".data_source"


def download_source() -> Path:
    CACHE_DIR.mkdir(exist_ok=True)
    zip_path = CACHE_DIR / "online_retail_ii.zip"
    xlsx_path = CACHE_DIR / "online_retail_II.xlsx"
    if not xlsx_path.exists():
        print(f"downloading {SOURCE_URL}")
        urlretrieve(SOURCE_URL, zip_path)
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(CACHE_DIR)
    return xlsx_path


def main() -> None:
    xlsx_path = download_source()
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    sheets = pd.read_excel(xlsx_path, sheet_name=None)
    transactions = pd.concat(sheets.values(), ignore_index=True)
    transactions.columns = [
        "invoice_id", "sku", "description", "quantity",
        "invoice_date", "unit_price", "customer_id", "country",
    ]
    transactions.to_csv(RAW_DIR / "sales.csv", index=False)
    print(f"wrote {len(transactions):,} rows to data/raw/sales.csv")

    # products: one row per real sku, the most common description observed
    # for it, and the min/max price ever charged, real data, not fabricated
    products = (
        transactions.groupby("sku")
        .agg(
            description=("description", lambda s: s.mode().iat[0] if not s.mode().empty else None),
            min_price=("unit_price", "min"),
            max_price=("unit_price", "max"),
            first_seen=("invoice_date", "min"),
            last_seen=("invoice_date", "max"),
        )
        .reset_index()
    )
    products.to_csv(RAW_DIR / "products.csv", index=False)
    print(f"wrote {len(products):,} rows to data/raw/products.csv")

    # calendar: a real date dimension generated from the real date range,
    # calendar tables are always derived, never a genuine raw extract
    dates = pd.date_range(
        transactions["invoice_date"].min().normalize(),
        transactions["invoice_date"].max().normalize(),
        freq="D",
    )
    calendar = pd.DataFrame({"date": dates})
    calendar["year"] = calendar["date"].dt.year
    calendar["month"] = calendar["date"].dt.month
    calendar["day"] = calendar["date"].dt.day
    calendar["week_of_year"] = calendar["date"].dt.isocalendar().week
    calendar["day_of_week"] = calendar["date"].dt.day_name()
    calendar["is_weekend"] = calendar["date"].dt.weekday >= 5
    calendar.to_csv(RAW_DIR / "calendar.csv", index=False)
    print(f"wrote {len(calendar):,} rows to data/raw/calendar.csv")


if __name__ == "__main__":
    main()
