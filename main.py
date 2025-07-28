# run_pipeline.py
import os
from data_pipeline.fetch.fetch_msedcl import fetch_msedcl_outage_data
from data_pipeline.clean.clean_msedcl import run_cleaning_pipeline
from data_pipeline.load.load_to_bq import run as load_to_bq
from utils.month_state import (
    get_last_month_tag,
    is_already_pushed,
    mark_as_pushed
)

def run_pipeline():
    print("🚀 Starting UrjaDrishti pipeline")
    month_tag = get_last_month_tag()  # e.g., '2025-06'

    print(f"📅 Target month: {month_tag}")

        # Step 2: Check if this month’s data was already pushed
    if is_already_pushed(month_tag):
        print(f"✅ Data for {month_tag} already pushed to BigQuery — skipping pipeline.")
        return

    print(f"📥 Fetching and processing data for {month_tag}...")

     # Step 3: Fetch data for the month
    try:
         raw_csv_path = fetch_msedcl_outage_data(month_tag)
    except Exception as e:
        print(f"Error fetching data for {month_tag}: {e}")
        return
    try:
        if raw_csv_path.empty:
            raise ValueError("No data fetched, raw_csv_path is empty.")
    except ValueError as ve:
        print(f"Error: {ve}")
        return
     # Step 4: Clean and transform the data
     #cleaned_file_path = run_cleaning_pipeline(month_tag, raw_csv_path)
    cleaned_file_path = run_cleaning_pipeline()[0]
    file_path = run_cleaning_pipeline()[1]
    try:
        if cleaned_file_path.empty:
            raise ValueError("No cleaned data file generated.")
        #Step 5: Load to BigQuery
        load_to_bq(file_path)
        # Step 6: Record successful load
        mark_as_pushed(month_tag)
        print(f"✅ Pipeline completed for {month_tag}. Data pushed to BigQuery and marked as processed.")
    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")
        return
    print(f"🚀 Pipeline execution for {month_tag} finished.")

    print("Pipeline execution completed.")

if __name__ == "__main__":
    run_pipeline()
