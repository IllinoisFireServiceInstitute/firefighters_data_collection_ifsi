import requests
import pandas as pd
from datetime import datetime

AIRTABLE_API_KEY = "patZVXpdjYOobb34k.6b343c1df9bb13b2fcb7b98c18a2cb273b90446d075d5b04538ce238f716791c"
BASE_ID = "appWUFr7x6vdKCjIj"
TABLE_NAME = "SampleProcessing"


def fetch_airtable_data():
    headers = {'Authorization': f'Bearer {AIRTABLE_API_KEY}'}
    url = f'https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}'
    all_records = []
    offset = None
    while True:
        params = {'offset': offset} if offset else {}
        response = requests.get(url, headers=headers, params=params).json()
        for record in response.get('records', []):
            fields = record.get('fields', {})
            biomatrix_raw = fields.get('Biomatrix', '')
            biomatrix_values = [b.strip() for b in biomatrix_raw.split(',')] if isinstance(biomatrix_raw, str) else [biomatrix_raw]

            for biomatrix in biomatrix_values:
                sample_record = {
                    'Part ID': fields.get('Participant Id', ''),
                    'Collection Date': fields.get('Collection date', ''),
                    'Processing Date': fields.get('Processing date', ''),
                    'Biomatrix': biomatrix,
                    'Initial': fields.get('Initial', '')
                }
                initial = sample_record['Initial']
                if isinstance(initial, list):
                    sample_record['Initial'] = ', '.join(initial)
                all_records.append(sample_record)
        offset = response.get('offset')
        if not offset:
            break
    return pd.DataFrame(all_records)

def explode_biomatrix_rows(df):
    df = df.copy()
    # Ensure list type
    df['Biomatrix'] = df['Biomatrix'].apply(lambda x: x if isinstance(x, list) else [x])
    df = df.explode('Biomatrix')
    df = df.reset_index(drop=True)
    return df


def updated_list_maker(df):
    part_list = df['Part ID'].unique()
    part_list_excel = pd.DataFrame(part_list, columns=['Part_ID'])
    part_list_excel = part_list_excel.sort_values(by='Part_ID')
    part_list_excel.to_excel('IFCRS_list_May_20.xlsx', index=False)


def excel_maker(df, updated_list, leftovers):
    part_list = df['Part ID'].unique()
    part_list = pd.DataFrame(part_list, columns=['Part_ID'])
    part_list = part_list[~part_list['Part_ID'].isin(updated_list)]

    for part_ID in part_list['Part_ID']:
        sample_ids = []
        collection_date = []
        processing_date = []
        IT = []
        sample_status = []
        storage_location = []
        Part_ID = []
        Sample_volume = []
        Biomatrix = []

        part_ID_df = df[df['Part ID'] == part_ID]

        def add_samples(bio_interest, count, prefix, vol, location, biomatrix_name, conditional_vols=None):
            for j in range(count):
                sample_id = f"{bio_interest.iloc[0, 0]}{prefix}{j + 1}"
                sample_ids.append(sample_id)
                collection_date.append(bio_interest.iloc[0, 1])
                processing_date.append(bio_interest.iloc[0, 2])
                IT.append(bio_interest.iloc[0, 4])
                sample_status.append('Stored')
                storage_location.append(location)
                Part_ID.append(part_ID)
                Biomatrix.append(biomatrix_name)
                if conditional_vols and j in conditional_vols:
                    Sample_volume.append(conditional_vols[j])
                else:
                    Sample_volume.append(vol)

        mapping = [
            ('WBG', 36, 'WBG', 0.5, '-80C Freezer#1', 'Whole blood green'),
            ('Plasma', 6, 'P_L', 0.5, '-80C Freezer#1', 'Plasma lavender'),
            ('Serum', 5, 'S', 0.6, '-80C Freezer#1', 'Serum'),
            ('RBC_BC', 3, 'RBC_BC', 1, '-80C Freezer#1', 'Red blood cell buffy coat'),
            ('Urine', 6, 'U', None, '-20C Chest Freezer#1', 'Urine')
        ]

        for biomatrix, count, prefix, vol, loc, name in mapping:
            bio_interest = part_ID_df[part_ID_df['Biomatrix'] == biomatrix]
            if not bio_interest.empty:
                conditional_vols = None
                if biomatrix == 'Urine':
                    conditional_vols = {5: 50}
                    vol = 10
                add_samples(bio_interest, count, prefix, vol, loc, name, conditional_vols)

        sample_ids_df = pd.DataFrame(sample_ids, columns=['Sample_ID'])
        sample_ids_df.insert(loc=0, column='Part_ID', value=Part_ID)
        sample_ids_df.insert(loc=2, column='Biomatrix', value=Biomatrix)
        sample_ids_df.insert(loc=3, column='Sample Volume (ml)', value=Sample_volume)
        sample_ids_df.insert(loc=4, column='Date_of_collection', value=collection_date)
        sample_ids_df.insert(loc=5, column='Sample processing date', value=processing_date)
        sample_ids_df.insert(loc=6, column='Initial of processors', value=IT)
        sample_ids_df.insert(loc=7, column='Sample storage location', value=storage_location)
        sample_ids_df.insert(loc=8, column='Sample Status', value=sample_status)

        new_df = sample_ids_df[~sample_ids_df['Sample_ID'].isin(leftovers)]
        new_df.to_excel(f'./tmpxls/IFCRS{part_ID}.xlsx', index=False)


# === Main Execution ===

df = fetch_airtable_data()
df = explode_biomatrix_rows(df)

leftovers = pd.read_excel('AFFI_leftover.xlsx')
leftovers = list(leftovers['Leftover'])


updated_list = pd.read_excel('IFCRS_list_May_20.xlsx')

# To regenerate participant list:
# updated_list_maker(df)

# To generate Excel files for participants:
excel_maker(df, updated_list['Part_ID'], leftovers)
