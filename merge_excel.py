import os
import pandas as pd
import argparse


def merge_excel_files(directory, output_file):
    all_dataframes = []

    for filename in os.listdir(directory):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(directory, filename)
            df = pd.read_excel(file_path, engine="openpyxl")

            # Coalesce processor columns
            if 'Initial of processor' in df.columns or 'Initial of processorr' in df.columns:
                # Create temporary columns with fallback values
                df['Processor'] = df.get('Initial of processor', pd.Series(index=df.index))
                df['Processor_temp'] = df.get('Initial of processorr', pd.Series(index=df.index))

                # Coalesce logic: prefer 'Initial of processor', fallback to 'Initial of processorr'
                df['Processor'] = df['Processor'].combine_first(df['Processor_temp'])

                # Drop temporary column and original columns
                df = df.drop(columns=['Processor_temp'], errors='ignore')
                df = df.drop(columns=['Initial of processor', 'Initial of processorr'], errors='ignore')

            all_dataframes.append(df)

    if all_dataframes:
        merged_df = pd.concat(all_dataframes, ignore_index=True)
        merged_df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
    else:
        print("No Excel files found in the directory.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge Excel files and coalesce processor columns")
    parser.add_argument("directory", help="Path to directory containing Excel files")
    parser.add_argument("output", help="Output CSV file path")

    args = parser.parse_args()
    merge_excel_files(args.directory, args.output)