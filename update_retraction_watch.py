import os
import pandas as pd
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from io import StringIO
import urllib.parse
import numpy as np

# Save service account credentials from secret to a file
if 'GOOGLE_CREDENTIALS_JSON' in os.environ:
    with open('credentials.json', 'w') as f:
        f.write(os.environ['GOOGLE_CREDENTIALS_JSON'])

CREDENTIALS_FILE = 'credentials.json'
SHEET_NAME = 'Retraction watch data'          
WORKSHEET_NAME = 'retraction_watch_29_may' 
CSV_URL = "https://gitlab.com/crossref/retraction-watch-data/-/raw/main/retraction_watch.csv"


def get_latest_csv():
    """Download the latest retraction_watch.csv from GitLab."""
    response = requests.get(CSV_URL)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))



def get_latest_commit_date():
    """Fetch the latest commit date for the retraction_watch.csv file from GitLab."""
    project_path = "crossref/retraction-watch-data"
    file_path = "retraction_watch.csv"
    encoded_project = urllib.parse.quote_plus(project_path)

    api_url = f"https://gitlab.com/api/v4/projects/{encoded_project}/repository/commits?path={file_path}&per_page=1"
    response = requests.get(api_url)
    response.raise_for_status()
    commit_info = response.json()[0]
    return commit_info['committed_date'][:10]  # Format: YYYY-MM-DD



def load_existing_data(sheet):
    """Read all existing records from the Google Sheet."""
    records = sheet.get_all_records()
    return pd.DataFrame(records)


def append_new_records(existing_df, latest_df, sheet, commit_date):
    """Compare by Record ID and append new entries with commit timestamp."""
    if 'Record ID' not in latest_df.columns:
        raise ValueError("CSV does not contain 'Record ID' column.")

    existing_ids = set(existing_df['Record ID']) if not existing_df.empty else set()
    new_rows = latest_df[~latest_df['Record ID'].isin(existing_ids)]
    
    if not new_rows.empty:
      new_rows = new_rows.copy()
      new_rows['timestamp'] = commit_date
      new_rows.replace([np.nan, np.inf, -np.inf], '', inplace=True)

      sheet.append_rows(new_rows.values.tolist(), value_input_option='USER_ENTERED')
      print(f"✅ Added {len(new_rows)} new rows with commit date: {commit_date}")
    else:
      print("✅ No new records found.")



def main():
    # Authenticate and connect to Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)

    sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)

    # Load data
    existing_df = load_existing_data(sheet)
    latest_df = get_latest_csv()
    commit_date = get_latest_commit_date()

    # Append new records with GitLab commit date
    append_new_records(existing_df, latest_df, sheet, commit_date)


# === ENTRY POINT ===
if __name__ == "__main__":
    main()



