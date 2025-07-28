import os
import pandas as pd
import re
from datetime import datetime,timezone
from dotenv import load_dotenv
import yaml

load_dotenv()
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
CLEAN_DATA_DIR = os.getenv("CLEAN_DATA_DIR")

with open("configs/settings.yaml") as f:
    settings = yaml.safe_load(f)

time_columns = settings["data_cleaning"]["time_columns"]
numeric_columns = settings["data_cleaning"]["numeric_columns"]
string_columns = settings["data_cleaning"]["string_columns"]

def convert_time_to_decimal(time_str):
    try:
        h, m = map(int, time_str.strip().split(":"))
        return round(h + m / 60, 2)
    except Exception:
        return None

def clean_text_column(text):
    if not isinstance(text, str):
        return text
    text = text.strip().upper()
    text = text.replace("DIVSION", "DIVISION")
    return re.sub(r"\s+", " ", text)

def clean_msedcl_dataframe(df):
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(clean_text_column)

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in time_columns:
        if col in df.columns:
            df[f"{col}_decimal"] = df[col].apply(convert_time_to_decimal)

    if "collected_at" in df.columns:
      df["collected_at"] = pd.to_datetime(df["collected_at"])
    else:
      df["collected_at"] = datetime.now(timezone.utc).isoformat()
    df["processed_at"] = datetime.now(timezone.utc).isoformat()
    return df

def load_latest_raw_csv():
    files = sorted([
        f for f in os.listdir(RAW_DATA_DIR)
        if f.endswith(".csv") and f.startswith("msedcl_raw")
    ], reverse=True)
    if not files:
        raise FileNotFoundError("No raw CSV file found in data/raw/")
    return os.path.join(RAW_DATA_DIR, files[0])

def save_cleaned_data(df, filename_prefix):
    os.makedirs(CLEAN_DATA_DIR, exist_ok=True)

    csv_path = os.path.join(CLEAN_DATA_DIR, f"{filename_prefix}.csv")
    json_path = os.path.join(CLEAN_DATA_DIR, f"{filename_prefix}.ndjson")
    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", lines=True, date_format="iso")

    print(f"[✓] Cleaned data saved to:\n  - {csv_path}\n  - {json_path}")
    return json_path

def run_cleaning_pipeline():
    input_file = load_latest_raw_csv()
    print(f"[ℹ] Cleaning file: {input_file}")
    df_raw = pd.read_csv(input_file)
    df_clean = clean_msedcl_dataframe(df_raw)

    month_tag = datetime.today().strftime("%Y-%m")
    filename_prefix = f"msedcl_clean_{month_tag}"
    json_path = save_cleaned_data(df_clean, filename_prefix)
    return df_clean,json_path

if __name__ == "__main__":
    run_cleaning_pipeline()
