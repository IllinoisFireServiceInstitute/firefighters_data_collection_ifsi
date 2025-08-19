import os
import argparse
import pandas as pd
import requests
from datetime import datetime
from urllib.parse import quote

# Airtable API details
API_KEY = "patZVXpdjYOobb34k.6b343c1df9bb13b2fcb7b98c18a2cb273b90446d075d5b04538ce238f716791c"
MAIN_BASE_ID = "appWUFr7x6vdKCjIj"
REFERENCE_BASE_ID = "appWUFr7x6vdKCjIj"
TABLE_NAME = "Samples"

REFERENCE_TABLES = {
    "Initial of collector": "Collectors",
    "Processor": "Processors",
    "Sample storage location": "StorageLocations"
}
REFERENCE_COL_NAMES = {
    "Initial of collector": "CollectorName",
    "Processor": "ProcessorID",
    "Sample storage location": "LocationName"
}
HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

def format_date(date_str):
    try:
        # If the date is already in the format YYYY-MM-DD, just return it
        if isinstance(date_str, str) and len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            return date_str
        # Otherwise, parse MM/DD/YY format and convert to YYYY-MM-DD
        return datetime.strptime(date_str, "%m/%d/%y").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None

def get_record_id(base_id, table_name, value, filter_col):
    value_escaped = str(value).replace("'", "''")
    formula = f"{{{filter_col}}}='{value_escaped}'"
    url_encoded_formula = quote(formula)
    url = f"https://api.airtable.com/v0/{base_id}/{table_name}?filterByFormula={url_encoded_formula}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        records = response.json().get("records", [])
        if records:
            return records[0]["id"]
    return None

def insert_record(base_id, table_name, value, filter_col):
    url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
    data = {"fields": {f"{filter_col}": value}}
    print(f"Inserting into {table_name} {filter_col}: {data}")
    response = requests.post(url, json=data, headers=HEADERS)
    if response.status_code == 200:
        return response.json().get("id")
    return None

def process_references(record):
    updated_record = {}
    for field, table in REFERENCE_TABLES.items():
        if field in record and not (pd.isna(record.get(field)) or record.get(field) is None):
            if field == "Processor":
                processors = str(record[field]).split(":")
                processor_ids = []
                for proc in processors:
                    proc = proc.strip()
                    if not proc:
                        continue
                    ref_id = get_record_id(REFERENCE_BASE_ID, table, proc, REFERENCE_COL_NAMES[field])
                    if not ref_id:
                        print(f"Processor '{proc}' not found, inserting...")
                        ref_id = insert_record(REFERENCE_BASE_ID, table, proc, REFERENCE_COL_NAMES[field])
                    if ref_id:
                        processor_ids.append(ref_id)
                updated_record[field] = processor_ids
            else:
                ref_value = record[field]
                ref_id = get_record_id(REFERENCE_BASE_ID, table, ref_value, REFERENCE_COL_NAMES[field])
                if not ref_id:
                    print(f"{field} '{ref_value}' not found, inserting...")
                    ref_id = insert_record(REFERENCE_BASE_ID, table, ref_value, REFERENCE_COL_NAMES[field])
                updated_record[field] = [ref_id] if ref_id else []
    return updated_record

def insert_main_record(record):
    url = f"https://api.airtable.com/v0/{MAIN_BASE_ID}/{TABLE_NAME}"

    def safe_get(field):
        value = record.get(field)
        if value is None or (not isinstance(value, list) and pd.isna(value)):
            return []
        return [str(x) for x in value] if isinstance(value, list) else [str(value)]

    fixed_record = {
        "Part ID": str(record.get("Part_ID", "")),
        "Sample ID": record.get("Sample_ID"),
        "Biomatrix": record.get("Biomatrix"),
        "Sample Volume (ml)": record.get("Sample Volume (ml)"),
        "Date of Collection": format_date(record.get("Date_of_collection")),
        "Sample Processing Date": format_date(record.get("Sample processing date")),
        "Sample Storage Location": safe_get("Sample storage location"),
        "Sample Status": [record.get("Sample Status")] if pd.notna(record.get("Sample Status")) else [],
        "Processor": safe_get("Processor")
    }
    cleaned_data = {
        k: v for k, v in fixed_record.items()
        if not ((isinstance(v, list) and len(v) == 0) or (not isinstance(v, list) and pd.isna(v)))
    }
    response = requests.post(url, json={"fields": cleaned_data}, headers=HEADERS)
    if response.status_code not in [200, 201]:
        print(f"Error {response.status_code}: {response.text}")
        return False
    return True

def main(input_file):
    df = pd.read_csv(input_file)
    for _, row in df.iterrows():
        record = row.to_dict()
        updated_references = process_references(record)
        record.update(updated_references)
        insert_main_record(record)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload firefighter data to Airtable from a CSV file.")
    parser.add_argument("input_file", type=str, help="Path to the CSV file containing firefighter data.")
    args = parser.parse_args()
    main(args.input_file)
