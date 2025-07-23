import os
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
import yaml

load_dotenv()
FETCH_URL = os.getenv("FETCH_URL")
USER_AGENT = os.getenv("USER_AGENT")

with open("configs/settings.yaml") as f:
    settings = yaml.safe_load(f)

RAW_DATA_DIR = settings["file_paths"]["raw_data_dir"]
DEFAULT_FETCH_TYPE = settings["default_fetch_type"]

def get_current_month_code():
    today = datetime.today()
    return f"{today.year % 100:02d}{today.month:02d}"

def fetch_msedcl_outage_data(fetch_month, fetch_type=DEFAULT_FETCH_TYPE):
    fetch_month = fetch_month or get_current_month_code()

    payload = {
        "fetchType": fetch_type,
        "fetchMonth": fetch_month
    }

    headers = {
        "User-Agent": USER_AGENT,
        "Content-Type": "application/x-www-form-urlencoded"
    }
   
    response = requests.post(FETCH_URL, data=payload, headers=headers)
    if response.status_code != 200:
        raise Exception(f"[✗] Failed to fetch data — HTTP {response.status_code}")
    try:
        raw_data = response.json()
        if len(raw_data) == 0:
            raise ValueError("Empty data received")
    except Exception as e:
        raise Exception(f"[✗] JSON parsing error: {e}")
    
    columns = [
        "region", "zone", "circle", "division",
    ]
    for week in range(1, 6):
        columns.extend([
            f"week{week}_total_outages",
            f"week{week}_duration_mins",
            f"week{week}_outage_hrs_per_feeder_day",
            f"week{week}_supply_hrs_per_feeder_day"
        ])

    df = pd.DataFrame(raw_data['data'], columns=columns[:len(raw_data['data'][0])])
    df["collected_at"] = datetime.now(timezone.utc).isoformat()
    df["fetch_month"] = (fetch_month)
    # print(df)
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    date_tag = datetime.today().strftime("%Y-%m-%d")

    csv_path = os.path.join(RAW_DATA_DIR, f"msedcl_raw_{fetch_month}_{date_tag}.csv")
    json_path = os.path.join(RAW_DATA_DIR, f"msedcl_raw_{fetch_month}_{date_tag}.json")

    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", indent=2)

    print(f"[✓] Raw data saved to:\n  - {csv_path}\n  - {json_path}")
    return df

if __name__ == "__main__":
    fetch_msedcl_outage_data()