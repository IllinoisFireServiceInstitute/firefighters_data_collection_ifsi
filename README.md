 # Firefighters Data Collection Scripts

This repository contains scripts and utilities for collecting, processing, and managing firefighters' data. The scripts streamline data acquisition, ensure accuracy, and support further analysis.

## Features
- **Automated Data Collection**: Scripts for uploading data into an Airtable base.
- **Data Preprocessing**: Cleaning and auto-adding referenced relations.
- **Logging and Error Handling**: Ensures reliability and debugging support.

## Prerequisites
- Python 3.x
- `pip`
- `nox`

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/IllinoisFireServiceInstitute/firefighters_data_collection_ifsi.git
cd firefighters_data_collection_ifsi
```

### 2. Create and Activate a Virtual Environment

```bash
python3 -m venv venv
```

On macOS/Linux:

```bash
source venv/bin/activate
```

On Windows:

```bash
venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```
### 4. Install Nox for Testing

```bash
pip install nox
```

## Using Nox

### Available Sessions
Run individual sessions:
```bash
nox -s setup_env
nox -s run_merge_excel
nox -s run_airtable_upload
```

Default Configuration

Merge Excel: Input directory /Users/niranjank/Downloads/updated/, output ./merged_data.csv
Airtable Upload: Uses ./merged_data.csv as input
