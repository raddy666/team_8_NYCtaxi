import pandas as pd
import os

BASE_FOLDER = "taxi_data"
OUTPUT_FOLDER = "taxi_data/processed"

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

def load_and_process_green(chunk_size=500000):
    folder_path = os.path.join(BASE_FOLDER, "green")
    all_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".csv")]

    data_chunks = []
    for file in all_files:
        print(f"Processing {file}...")
        for chunk in pd.read_csv(file, chunksize=chunk_size, low_memory=False):
            rename_cols = {
                "lpep_pickup_datetime": "pickup_time",
                "lpep_dropoff_datetime": "dropoff_time",
                "PULocationID": "pickup_location",
                "DOLocationID": "dropoff_location",
                "trip_distance": "distance",
                "fare_amount": "fare",
                "tip_amount": "tips",
                "tolls_amount": "tolls",
                "total_amount": "total_fare",
            }
            chunk.rename(columns=rename_cols, inplace=True)

            # Convert Data Types
            chunk["pickup_time"] = pd.to_datetime(chunk["pickup_time"], errors="coerce")
            chunk["dropoff_time"] = pd.to_datetime(chunk["dropoff_time"], errors="coerce")
            chunk["distance"] = pd.to_numeric(chunk["distance"], errors="coerce", downcast="float")
            chunk["fare"] = pd.to_numeric(chunk["fare"], errors="coerce", downcast="float")
            chunk["tips"] = pd.to_numeric(chunk["tips"], errors="coerce", downcast="float")
            chunk["tolls"] = pd.to_numeric(chunk["tolls"], errors="coerce", downcast="float")
            chunk["total_fare"] = pd.to_numeric(chunk["total_fare"], errors="coerce", downcast="float")

            # Add a taxi_type column (since it's a Green taxi, we assign 'green' as the type)
            chunk['taxi_type'] = 'green'

            data_chunks.append(chunk)

    # Concatenate all chunks into a single DataFrame
    df = pd.concat(data_chunks, ignore_index=True)

    # Save processed data as Parquet
    output_path = os.path.join(OUTPUT_FOLDER, "green_processed.parquet")
    df.to_parquet(output_path, compression="snappy")
    print(f"Saved green data to {output_path}")
    return df

if __name__ == "__main__":
    load_and_process_green()
