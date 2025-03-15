import pandas as pd
import os

BASE_FOLDER = "taxi_data"
OUTPUT_FOLDER = "taxi_data/processed"

# Create the processed folder if it doesn't exist
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)
    print(f"Created folder: {OUTPUT_FOLDER}")
else:
    print(f"Folder already exists: {OUTPUT_FOLDER}")

def load_and_process_yellow(chunk_size=500000):
    folder_path = os.path.join(BASE_FOLDER, "yellow")
    
    # Verify that the correct folder and files exist
    all_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".csv")]
    print(f"Found the following files: {all_files}")
    
    if not all_files:
        print("No CSV files found in the 'yellow' folder.")
        return None  # Exit if no files found

    data_chunks = []
    for file in all_files:
        print(f"Processing {file}...")
        for chunk in pd.read_csv(file, chunksize=chunk_size, low_memory=False):
            # Rename columns based on the schema for yellow taxi data
            rename_cols = {
                "Trip_Pickup_DateTime": "pickup_time",
                "Trip_Dropoff_DateTime": "dropoff_time",
                "Passenger_Count": "passenger_count",
                "Trip_Distance": "trip_distance",
                "Start_Lon": "pickup_lon",
                "Start_Lat": "pickup_lat",
                "Rate_Code": "rate_code",
                "store_and_forward": "store_and_forward",
                "End_Lon": "dropoff_lon",
                "End_Lat": "dropoff_lat",
                "Payment_Type": "payment_type",
                "Fare_Amt": "fare_amount",
                "surcharge": "surcharge",
                "mta_tax": "mta_tax",
                "Tip_Amt": "tip_amount",
                "Tolls_Amt": "tolls_amount",
                "Total_Amt": "total_amount"
            }
            chunk.rename(columns=rename_cols, inplace=True)

            # Convert data types
            chunk["pickup_time"] = pd.to_datetime(chunk["pickup_time"], errors="coerce")
            chunk["dropoff_time"] = pd.to_datetime(chunk["dropoff_time"], errors="coerce")
            chunk["passenger_count"] = pd.to_numeric(chunk["passenger_count"], errors="coerce", downcast="integer")
            chunk["trip_distance"] = pd.to_numeric(chunk["trip_distance"], errors="coerce", downcast="float")
            chunk["pickup_lon"] = pd.to_numeric(chunk["pickup_lon"], errors="coerce", downcast="float")
            chunk["pickup_lat"] = pd.to_numeric(chunk["pickup_lat"], errors="coerce", downcast="float")
            chunk["dropoff_lon"] = pd.to_numeric(chunk["dropoff_lon"], errors="coerce", downcast="float")
            chunk["dropoff_lat"] = pd.to_numeric(chunk["dropoff_lat"], errors="coerce", downcast="float")
            chunk["rate_code"] = pd.to_numeric(chunk["rate_code"], errors="coerce", downcast="integer")
            chunk["store_and_forward"] = chunk["store_and_forward"].astype(str)
            chunk["payment_type"] = chunk["payment_type"].astype(str)
            chunk["fare_amount"] = pd.to_numeric(chunk["fare_amount"], errors="coerce", downcast="float")
            chunk["surcharge"] = pd.to_numeric(chunk["surcharge"], errors="coerce", downcast="float")
            chunk["mta_tax"] = pd.to_numeric(chunk["mta_tax"], errors="coerce", downcast="float")
            chunk["tip_amount"] = pd.to_numeric(chunk["tip_amount"], errors="coerce", downcast="float")
            chunk["tolls_amount"] = pd.to_numeric(chunk["tolls_amount"], errors="coerce", downcast="float")
            chunk["total_amount"] = pd.to_numeric(chunk["total_amount"], errors="coerce", downcast="float")

            # Add taxi type column for identification
            chunk["taxi_type"] = "yellow"

            data_chunks.append(chunk)

    df = pd.concat(data_chunks, ignore_index=True)

    # Save processed data as Parquet (MUCH FASTER than CSV)
    output_path = os.path.join(OUTPUT_FOLDER, "yellow_processed.parquet")
    print(f"Output path: {output_path}")  # Print the output path
    
    # Try to save and catch any errors
    try:
        df.to_parquet(output_path, compression="snappy")
        print(f"Saved Yellow taxi data to {output_path}")
    except Exception as e:
        print(f"Error saving file: {e}")

if __name__ == "__main__":
    # Print current working directory
    print(f"Current working directory: {os.getcwd()}") 
    
    # Start the processing
    load_and_process_yellow()
