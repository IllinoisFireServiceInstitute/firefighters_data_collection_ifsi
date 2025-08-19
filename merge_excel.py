import os
import pandas as pd
import argparse


def merge_excel_files(directory, output_file):
    all_dataframes = []

    for filename in os.listdir(directory):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(directory, filename)

            # Read all columns as strings, allow empty values (NaN stays as NaN)
            df = pd.read_excel(file_path, engine="openpyxl", dtype=str, keep_default_na=False, na_values=[""])

            # Strip column names just in case
            df.columns = [col.strip() for col in df.columns]

            # Processor initials handling
            processor_col = None
            if 'Initial of processors' in df.columns:
                processor_col = df['Initial of processors']
            elif 'Initial of processorr' in df.columns:
                processor_col = df['Initial of processorr']

            if processor_col is not None:
                def clean_initials(val):
                    if pd.isna(val) or val.strip() == "":
                        return ""
                    initials = [x.strip() for x in str(val).split(',')]
                    return ':'.join(initials)

                df['Processor'] = processor_col.apply(clean_initials)

            # Drop original processor columns
            df = df.drop(columns=['Initial of processors', 'Initial of processorr'], errors='ignore')

            # Handle empty 'Processing Date' column
            if 'Processing Date' in df.columns:
                # Option 1: Fill empty 'Processing Date' with a placeholder (e.g., 'Not Provided')
                df['Processing Date'].fillna('Not Provided', inplace=True)

                # Option 2: Alternatively, if you want to remove rows where 'Processing Date' is empty
                # df = df[df['Processing Date'].notna()]

            all_dataframes.append(df)

    if all_dataframes:
        merged_df = pd.concat(all_dataframes, ignore_index=True)

        # Save to CSV, keeping empty strings as-is
        merged_df.to_csv(output_file, index=False, na_rep='')
        print(f"✅ Data saved to {output_file}")
    else:
        print("⚠️ No Excel files found in the directory.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge Excel files and clean processor initials")
    parser.add_argument("directory", help="Path to directory containing Excel files")
    parser.add_argument("output", help="Output CSV file path")

    args = parser.parse_args()
    merge_excel_files(args.directory, args.output)
