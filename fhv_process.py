import pandas as pd
import os

BASE_FOLDER = "taxi_data"
OUTPUT_FOLDER = "taxi_data/processed"

# Create the processed folder if it doesn't exist
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

def load_and_process_fhv(chunk_size=500000):
    folder_path = os.path.join(BASE_FOLDER, "fhv")
    all_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".csv")]

    data_chunks = []
    for file in all_files:
        print(f"Processing {file}...")
        for chunk in pd.read_csv(file, chunksize=chunk_size, low_memory=False):
            # Rename columns based on the schema_fhv
            rename_cols = {
                "pickup_datetime": "pickup_time",
                "dropOff_datetime": "dropoff_time",
                "PUlocationID": "pickup_location",
                "DOlocationID": "dropoff_location",
                "SR_Flag": "shared_ride_flag",  # Assuming this flag indicates shared rides
                "Affiliated_base_number": "affiliated_base",
                # Add any other necessary renaming or handling of columns
            }
            chunk.rename(columns=rename_cols, inplace=True)

            # Convert data types
            chunk["pickup_time"] = pd.to_datetime(chunk["pickup_time"], errors="coerce")
            chunk["dropoff_time"] = pd.to_datetime(chunk["dropoff_time"], errors="coerce")
            chunk["pickup_location"] = pd.to_numeric(chunk["pickup_location"], errors="coerce", downcast="float")
            chunk["dropoff_location"] = pd.to_numeric(chunk["dropoff_location"], errors="coerce", downcast="float")
            chunk["shared_ride_flag"] = pd.to_numeric(chunk["shared_ride_flag"], errors="coerce", downcast="integer")
            chunk["affiliated_base"] = chunk["affiliated_base"].astype(str)

            # Add taxi type column for identification
            chunk["taxi_type"] = "fhv"

            data_chunks.append(chunk)

    df = pd.concat(data_chunks, ignore_index=True)

    # Save processed data as Parquet (MUCH FASTER than CSV)
    output_path = os.path.join(OUTPUT_FOLDER, "fhv_processed.parquet")
    df.to_parquet(output_path, compression="snappy")
    print(f"Saved FHV data to {output_path}")
    return df

if __name__ == "__main__":
    load_and_process_fhv()
