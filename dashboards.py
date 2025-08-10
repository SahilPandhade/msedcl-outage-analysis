import os
import json
import pandas as pd
import streamlit as st
import plotly.express as px
from google.cloud import bigquery
from dotenv import load_dotenv
from google.oauth2 import service_account

# Load environment variables
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID") or st.secrets["PROJECT_ID"]
DATASET = os.getenv("DATASET_NAME") or st.secrets["DATASET_NAME"]
TABLE = os.getenv("TABLE_NAME") or st.secrets["TABLE_NAME"]
raw_secret = st.secrets["GOOGLE_APPLICATION_CREDENTIALS_JSON"]

def get_gcp_credentials():
    # Case 1: Running locally with GOOGLE_APPLICATION_CREDENTIALS
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return None  # BigQuery will auto-use the env variable

    # Case 2: Running on Streamlit Cloud, use st.secrets
    if "gcp_service_account" in st.secrets:
        if isinstance(raw_secret, str):
            creds_dict = json.loads(raw_secret,strict=False)
        elif isinstance(raw_secret, dict):
            creds_dict = raw_secret
        else:
            raise ValueError("Unexpected format for gcp_service_account secret")
        return service_account.Credentials.from_service_account_info(creds_dict)

    raise RuntimeError("No GCP credentials found in environment or secrets.")

# Get credentials
creds = get_gcp_credentials()
# Load JSON from Streamlit secrets
#service_account_info = json.loads(st.secrets["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
# Authenticate
#credentials = service_account.Credentials.from_service_account_info(service_account_info)

st.set_page_config(page_title="UrjaDrishti Dashboard", layout="wide")
st.title("⚡ UrjaDrishti: Maharashtra Grid Health Dashboard")

# Load data from BigQuery
@st.cache_data(ttl=3600)
def load_data():
    if creds:
        client = bigquery.Client(credentials=creds, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE fetch_month IS NOT NULL
    """
    return client.query(query).to_dataframe()

df = load_data()

# ===================== Filters =====================
st.sidebar.header("🔍 Filters")

regions = df["region"].dropna().unique().tolist()
zones = df["zone"].dropna().unique().tolist()
divisions = df["division"].dropna().unique().tolist()

selected_region = st.sidebar.selectbox("Region", ["All"] + sorted(regions))
selected_zone = st.sidebar.selectbox("Zone", ["All"] + sorted(zones))
selected_division = st.sidebar.selectbox("Division", ["All"] + sorted(divisions))

if selected_region != "All":
    df = df[df["region"] == selected_region]
if selected_zone != "All":
    df = df[df["zone"] == selected_zone]
if selected_division != "All":
    df = df[df["division"] == selected_division]

# ===================== KPIs =====================
st.subheader("📈 Key Performance Indicators")

latest_month = df["fetch_month"].max()
latest_data = df[df["fetch_month"] == latest_month]

total_outages = latest_data[[f"week{i}_total_outages" for i in range(1, 6)]].sum(axis=1).sum()
total_supply = latest_data[[f"week{i}_supply_hrs_per_feeder_day_decimal" for i in range(1, 6)]].sum(axis=1).mean()
total_outage = latest_data[[f"week{i}_outage_hrs_per_feeder_day_decimal" for i in range(1, 6)]].sum(axis=1).mean()

col1, col2, col3 = st.columns(3)
col1.metric("🛠️ Total Outages", f"{int(total_outages)}")
col2.metric("🔌 Avg Supply Hrs/Feeder/Day", f"{total_supply:.2f}")
col3.metric("⚠️ Avg Outage Hrs/Feeder/Day", f"{total_outage:.2f}")

# ===================== Weekly Trends =====================
def melt_weeks(df):
    week_cols = [col for col in df.columns if "supply_hrs_per_feeder_day_decimal" in col or "outage_hrs_per_feeder_day_decimal" in col]
    df_long = df.melt(id_vars=["fetch_month", "region", "zone", "division", "circle"], value_vars=week_cols,
                      var_name="metric", value_name="hours")
    df_long["metric_type"] = df_long["metric"].apply(lambda x: "Supply" if "supply" in x else "Outage")
    df_long["week"] = df_long["metric"].str.extract(r"week(\d)").astype(str)
    return df_long

df_long = melt_weeks(df)

# st.subheader("📊 Weekly Supply vs Outage Breakdown")

# # Step 1: Convert fetch_month like 2506 -> "2025-06"
# df_long["fetch_month_str"] = df_long["fetch_month"].apply(lambda x: f"20{x//100:02d}-{x%100:02d}")

# # Step 2: Group and plot
# weekly_avg = df_long.groupby(["fetch_month_str", "week", "metric_type"])["hours"].mean().reset_index()

# fig = px.bar(
#     weekly_avg,
#     x="fetch_month_str",
#     y="hours",
#     color="metric_type",
#     barmode="group",
#     facet_col="week",
#     facet_col_wrap=3,
#     title="Avg Weekly Supply vs Outage Hrs (Per Feeder/Day)",
#     labels={"fetch_month_str": "Month", "hours": "Hrs/Feeder/Day"}
# )

# fig.update_layout(xaxis_tickangle=-45)
# st.plotly_chart(fig, use_container_width=True)


# ===================== Reliability Score =====================
st.subheader("💡 Reliability Score by Division (Top 10)")
def compute_reliability_score(df):
    df["reliability_score"] = df[
        [f"week{i}_supply_hrs_per_feeder_day_decimal" for i in range(1, 6)]
    ].sum(axis=1) - df[
        [f"week{i}_outage_hrs_per_feeder_day_decimal" for i in range(1, 6)]
    ].sum(axis=1)
    return df

df = compute_reliability_score(df)
reliability = df.groupby("division")["reliability_score"].mean().reset_index().sort_values(by="reliability_score", ascending=False).head(10)

fig2 = px.bar(reliability, x="division", y="reliability_score", title="Division-wise Avg Reliability Score",
              labels={"reliability_score": "Supply - Outage (hrs)"})
st.plotly_chart(fig2, use_container_width=True)

# ===================== Total Outage Duration =====================
st.subheader("🛑 Total Outage Minutes per Month")
df["total_outage_mins"] = df[[f"week{i}_duration_mins" for i in range(1, 6)]].sum(axis=1)
df["fetch_month_str"] = df["fetch_month"].apply(lambda x: f"20{x//100:02d}-{x%100:02d}")
outage_monthly = df.groupby("fetch_month_str")["total_outage_mins"].sum().reset_index()

fig3 = px.area(outage_monthly, x="fetch_month_str", y="total_outage_mins",
               title="Total Outage Minutes (All Divisions Combined)")
st.plotly_chart(fig3, use_container_width=True)

# ===================== Optional: Raw Table =====================
with st.expander("🧾 Show raw data (first 100 rows)"):
    st.write(df.head(100))
