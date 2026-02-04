#!/usr/bin/env python3
"""
Clean FATURA-style template CSVs.

INPUT (raw):
  data/csv_files/raw_data/template1.csv ... template5.csv

OUTPUT (cleaned):
  data/csv_files/cleaned_data/template1_clean.csv ... template5_clean.csv
"""

import os
import re
from datetime import datetime
import pandas as pd

pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 180)

RAW_DIR = os.path.join("../", "data", "csv_files", "raw_data")
CLEAN_DIR = os.path.join("../", "data", "csv_files", "cleaned_data")


# -------------------------------------------------
# Helper functions
# -------------------------------------------------
_CURRENCY_MAP = {"€": "EUR", "$": "USD", "₹": "INR", "£": "GBP"}

def clean_spaces(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return re.sub(r"\s+", " ", str(x)).strip()


def parse_amount_and_currency(text):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None, None

    t = clean_spaces(text)
    if not t:
        return None, None

    # Currency detection
    currency = None
    code = re.search(r"\b(USD|EUR|INR|GBP|AED|SGD|AUD|CAD|JPY|CNY)\b", t, re.I)
    if code:
        currency = code.group(1).upper()
    else:
        symbol = re.search(r"[€$₹£]", t)
        if symbol:
            currency = _CURRENCY_MAP.get(symbol.group(0))

    # Amount: take the last numeric token
    nums = re.findall(r"[-+]?\d{1,3}(?:,\d{3})*(?:\.\d+)?|[-+]?\d+(?:\.\d+)?", t)
    if not nums:
        return None, currency

    try:
        amount = float(nums[-1].replace(",", ""))
    except ValueError:
        return None, currency

    # Discount handling
    if "DISCOUNT" in t.upper() or "(-)" in t or re.search(r"\(\s*-\s*\)", t):
        amount = -abs(amount)

    return amount, currency


def normalize_date(text):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None

    t = clean_spaces(text)
    if not t:
        return None

    if ":" in t:
        t = t.split(":", 1)[1].strip()

    formats = [
        "%d-%b-%Y", "%d-%B-%Y",
        "%d/%m/%Y", "%d-%m-%Y",
        "%Y-%m-%d",
        "%m/%d/%Y",
    ]

    for f in formats:
        try:
            return datetime.strptime(t, f).strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None


def split_address_block(text):
    result = {
        "name": None,
        "addr_line1": None,
        "addr_line2": None,
        "city_state_postal_country": None,
        "tel": None,
        "email": None,
        "site": None,
        "raw": None,
    }

    if text is None or (isinstance(text, float) and pd.isna(text)):
        return result

    lines = [l.strip() for l in str(text).splitlines() if l.strip()]
    if not lines:
        return result

    result["raw"] = " | ".join(lines) # type: ignore

    for line in lines:
        low = line.lower()
        if low.startswith("tel"):
            result["tel"] = line.split(":", 1)[-1].strip() if ":" in line else line # pyright: ignore[reportArgumentType]
        elif low.startswith("email"):
            result["email"] = line.split(":", 1)[-1].strip() if ":" in line else line # pyright: ignore[reportArgumentType]
        elif low.startswith("site") or "http" in low or low.startswith("www"):
            result["site"] = line.split(":", 1)[-1].strip() if ":" in line else line # pyright: ignore[reportArgumentType]

    addr = [l for l in lines if not l.lower().startswith(("tel", "email", "site"))]

    if addr:
        result["name"] = addr[0] # pyright: ignore[reportArgumentType]
        if len(addr) > 1:
            result["addr_line1"] = addr[1] # type: ignore
        if len(addr) > 2:
            result["addr_line2"] = addr[2] # type: ignore
        if len(addr) > 3:
            result["city_state_postal_country"] = addr[3] # pyright: ignore[reportArgumentType]

    return result


# -------------------------------------------------
# Core cleaning logic
# -------------------------------------------------
def clean_template_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Drop noisy columns
    for col in ["OTHER", "LOGO"]:
        if col in df.columns:
            df.drop(columns=col, inplace=True)

    # Amount parsing + currency
    amount_map = {
        "TOTAL": "TOTAL_AMOUNT",
        "SUB_TOTAL": "SUB_TOTAL_AMOUNT",
        "TAX": "TAX_AMOUNT",
        "DISCOUNT": "DISCOUNT_AMOUNT",
    }

    currency_series = []
    for src, tgt in amount_map.items():
        if src in df.columns:
            parsed = df[src].apply(parse_amount_and_currency)
            df[tgt] = parsed.apply(lambda x: x[0])
            currency_series.append(parsed.apply(lambda x: x[1]))
        else:
            df[tgt] = None

    if currency_series:
        cur_df = pd.concat(currency_series, axis=1)
        df["CURRENCY"] = cur_df.bfill(axis=1).iloc[:, 0]
    else:
        df["CURRENCY"] = None

    # Date normalization
    if "DATE" in df.columns:
        df["DATE_NORM"] = df["DATE"].apply(normalize_date)
    if "DUE_DATE" in df.columns:
        df["DUE_DATE_NORM"] = df["DUE_DATE"].apply(normalize_date)

    # Address parsing
    if "BUYER" in df.columns:
        buyer_df = pd.json_normalize(df["BUYER"].apply(split_address_block)).add_prefix("BUYER_") # type: ignore
        df = pd.concat([df, buyer_df], axis=1)

    if "SELLER_ADDRESS" in df.columns:
        seller_df = pd.json_normalize(df["SELLER_ADDRESS"].apply(split_address_block)).add_prefix("SELLER_") # type: ignore
        df = pd.concat([df, seller_df], axis=1)

    return df.reindex(sorted(df.columns), axis=1)


# -------------------------------------------------
# Main execution
# -------------------------------------------------
def main():
    os.makedirs(CLEAN_DIR, exist_ok=True)

    for i in range(1, 6):
        raw_path = os.path.join(RAW_DIR, f"template{i}.csv")
        out_path = os.path.join(CLEAN_DIR, f"template{i}_clean.csv")

        if not os.path.isfile(raw_path):
            continue

        df = pd.read_csv(raw_path)
        clean_df = clean_template_df(df)
        clean_df.to_csv(out_path, index=False, encoding="utf-8")


if __name__ == "__main__":
    main()