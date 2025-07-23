import os
from datetime import datetime, timedelta

STATE_FILE = ".state/pushed_months.txt"

def get_last_month_tag():
    today = datetime.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    return "2505"
    #return last_month.strftime("%Y-%m")

def is_already_pushed(month_tag):
    if not os.path.exists(STATE_FILE):
        return False
    with open(STATE_FILE, "r") as f:
        pushed_months = f.read().splitlines()
    return month_tag in pushed_months

def mark_as_pushed(month_tag):
    os.makedirs(".state", exist_ok=True)
    with open(STATE_FILE, "a") as f:
        f.write(f"{month_tag}\n")
