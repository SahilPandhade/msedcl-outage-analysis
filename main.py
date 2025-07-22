import requests
import pandas as pd
from datetime import datetime
import os

def fetch_msedcl_outage_json(fetch_month="2505", fetch_type="SUPPLYHR_DATA"):
    url = "https://www.mahadiscom.in/consumer/ajax/fetch_divisionwise_outage.php"
    
    payload = {
        "fetchType": fetch_type,
        "fetchMonth": fetch_month
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    response = requests.post(url, data=payload, headers=headers)

    if response.status_code != 200:
        print(f"[✗] Failed to fetch data: {response.status_code}")
        return pd.DataFrame()

    try:
        raw_data = response.json()
    except Exception:
        print("[✗] Failed to parse JSON.")
        return pd.DataFrame()

    columns = [
        "region",
        "zone",
        "circle",
        "division",
    ]

    for week in range(1, 6):
        columns.extend([
            f"week{week}_total_outages",
            f"week{week}_duration_mins",
            f"week{week}_outage_hrs_per_feeder_day",
            f"week{week}_supply_hrs_per_feeder_day"
        ])

    df = pd.DataFrame(raw_data['data'], columns=columns[:len(raw_data['data'][0])])

    df["collected_at"] = datetime.utcnow().isoformat()

    print(f"Total records: {len(df)}")
    return df

def convert_time_to_decimal(time_str):
    """Convert HH:MM to float hours"""
    try:
        h, m = map(int, time_str.split(":"))
        return round(h + m / 60, 2)
    except:
        return None

def clean_msedcl_data(df):
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    for col in ["region", "zone", "circle", "division"]:
        df[col] = df[col].str.upper().str.replace(r'\s+', ' ', regex=True)
        df[col] = df[col].str.replace("DIVSION", "DIVISION", regex=False)
    for week in range(1, 6):
        df[f"week{week}_total_outages"] = pd.to_numeric(df[f"week{week}_total_outages"], errors="coerce").fillna(0).astype(int)
        df[f"week{week}_duration_mins"] = pd.to_numeric(df[f"week{week}_duration_mins"], errors="coerce").fillna(0).astype(int)

        df[f"week{week}_outage_hrs_per_feeder_day_decimal"] = df[f"week{week}_outage_hrs_per_feeder_day"].apply(convert_time_to_decimal)
        df[f"week{week}_supply_hrs_per_feeder_day_decimal"] = df[f"week{week}_supply_hrs_per_feeder_day"].apply(convert_time_to_decimal)
    df["collected_at"] = pd.to_datetime(df["collected_at"], errors="coerce")

    return df

if __name__ == "__main__":
    # df = fetch_msedcl_outage_json()
    # if not df.empty:
    #     os.makedirs("local_data", exist_ok=True)
    #     filename = f"local_data/msedcl_outages_{datetime.now().date()}.json"
    #     df.to_json(filename, orient="records", indent=2)
    #     print(f"{filename} created.")
    
    # Load JSON
    df_raw = pd.read_json("local_data/msedcl_outages_2025-07-13.json")
    # #os.makedirs("clean_data", exist_ok=True)
    df_clean = clean_msedcl_data(df_raw)

    # csv_path = "clean_data/msedcl_outages_clean.csv"
    # df_clean.to_csv(csv_path, index=False)
    # json_path = "clean_data/msedcl_outages_clean.ndjson"
    # df_clean.to_json(json_path, orient="records", indent=2, date_format="iso")
    # print(f"[✓] Saved cleaned JSON to: {json_path}")

    # Total outages summed across all 5 weeks per division
    # agg_df = df_clean.groupby("division")[
    #     [f"week{w}_total_outages" for w in range(1, 6)]
    # ].sum()

    # agg_df["total_outages"] = agg_df.sum(axis=1)
    # agg_df = agg_df.sort_values("total_outages", ascending=False)

    # print("\nTop divisions by total outages:")
    # print(agg_df.head(10))
