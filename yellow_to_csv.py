import pandas as pd
import glob

file_pattern = 'dataset/Yellow/yellow_tripdata_*.parquet'

files = glob.glob(file_pattern)

output_csv = "dataset/Yellow/yellow_merged_csv.csv"

for i, file in enumerate(files):
    df = pd.read_parquet(file, engine="pyarrow")

    if i == 0:
        df.to_csv(output_csv, index=False, mode='w', header=True)
    else:
        df.to_csv(output_csv, index=False, mode='a', header=False)
    print(f"Successfully processed and appended: {file}")

print("CSV file saved successfully!")
