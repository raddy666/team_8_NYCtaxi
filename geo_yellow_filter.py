import glob
import pandas as pd
import os

selected_columns = ["Start_Lon", "Start_Lat", "End_Lon", "End_Lat"]
dtype_dict = {col: float for col in selected_columns}

input_folder = "geo_yellow/"
output_folder = "processed_data/"
os.makedirs(output_folder, exist_ok=True)

csv_files = glob.glob(os.path.join(input_folder, "*.csv"))

df_list = []

for file in csv_files:
    try:
        df = pd.read_csv(file, usecols=selected_columns, dtype=dtype_dict)

        df_cleaned = df[
            df["Start_Lat"].between(-90, 90) & df["End_Lat"].between(-90, 90) &
            df["Start_Lon"].between(-180, 180) & df["End_Lon"].between(-180, 180)
            ]

        filename = os.path.basename(file)
        output_file = os.path.join(output_folder, f"processed_{filename}")
        df_cleaned.to_csv(output_file, index=False)

        df_list.append(df_cleaned)

    except ValueError:
        print(f"Skipping {file} due to missing columns.")

if df_list:
    merged_df = pd.concat(df_list, ignore_index=True)
    merged_df.to_csv(os.path.join(output_folder, "processed_data.csv"), index=False)
    print("merged")


