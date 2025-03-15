import pandas as pd
import os

BASE_FOLDER = "taxi_data"
OUTPUT_FOLDER = "taxi_data/processed"

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

def load_and_process_fhvhv(chunk_size=500000):
    folder_path = os.path.join(BASE_FOLDER, "fhvhv")
    all_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".csv")]

    data_chunks = []
    for file in all_files:
        print(f"Processing {file}...")
        for chunk in pd.read_csv(file, chunksize=chunk_size, low_memory=False):
            rename_cols = {
                "pickup_datetime": "pickup_time",
                "dropoff_datetime": "dropoff_time",
                "PULocationID": "pickup_location",
                "DOLocationID": "dropoff_location",
                "trip_miles": "distance",
                "base_passenger_fare": "fare",
                "tips": "tip_amount",
            }
            chunk.rename(columns=rename_cols, inplace=True)

            # Convert Data Types
            chunk["pickup_time"] = pd.to_datetime(chunk["pickup_time"], errors="coerce")
            chunk["dropoff_time"] = pd.to_datetime(chunk["dropoff_time"], errors="coerce")
            chunk["distance"] = pd.to_numeric(chunk["distance"], errors="coerce", downcast="float")
            chunk["fare"] = pd.to_numeric(chunk["fare"], errors="coerce", downcast="float")
            chunk["tip_amount"] = pd.to_numeric(chunk["tip_amount"], errors="coerce", downcast="float")

            # Add a taxi_type column (since it's FHVHV taxi, we assign 'fhvhv' as the type)
            chunk['taxi_type'] = 'fhvhv'

            data_chunks.append(chunk)

    # Concatenate all chunks into a single DataFrame
    df = pd.concat(data_chunks, ignore_index=True)

    # Save processed data as Parquet
    output_path = os.path.join(OUTPUT_FOLDER, "fhvhv_processed.parquet")
    df.to_parquet(output_path, compression="snappy")
    print(f"Saved FHVHV data to {output_path}")
    return df

if __name__ == "__main__":
    load_and_process_fhvhv()
