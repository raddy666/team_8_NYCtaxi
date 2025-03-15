import dask.dataframe as dd
import pandas as pd

# Define the feature engineering function for chunk processing
def feature_engineering(df, taxi_type_column=None):
    # Ensure pickup_time and dropoff_time are in datetime format
    df['pickup_time'] = pd.to_datetime(df['pickup_time'], errors='coerce')
    df['dropoff_time'] = pd.to_datetime(df['dropoff_time'], errors='coerce')

    # Drop rows with missing values in 'pickup_time' or 'dropoff_time'
    df = df.dropna(subset=['pickup_time', 'dropoff_time'])

    # 1. Extract day of week from pickup time (0=Monday, 6=Sunday)
    df['pickup_day_of_week'] = df['pickup_time'].dt.dayofweek

    # 2. Extract hour of the day from pickup time (0=midnight, 23=11pm)
    df['pickup_hour'] = df['pickup_time'].dt.hour

    # 3. Calculate trip duration in minutes
    df['trip_duration'] = (df['dropoff_time'] - df['pickup_time']).dt.total_seconds() / 60.0

    # 4. Convert taxi_type to numeric if it's available in the dataset
    if taxi_type_column and taxi_type_column in df.columns:
        df = pd.get_dummies(df, columns=[taxi_type_column], drop_first=True)

    return df

# Example for loading and processing the dataset in chunks with Dask
def process_in_chunks_with_dask(file_path, taxi_type_column=None):
    # Load the Parquet file using Dask (this reads it as a Dask dataframe)
    ddf = dd.read_parquet(file_path)

    # Apply feature engineering on the entire Dask dataframe
    ddf = ddf.map_partitions(feature_engineering, taxi_type_column)

    # Convert Dask dataframe back to pandas and return
    return ddf.compute()

# Load and process the datasets in chunks using Dask
yellow_df = process_in_chunks_with_dask("taxi_data/processed/yellow_processed.parquet", taxi_type_column='taxi_type')
green_df = process_in_chunks_with_dask("taxi_data/processed/green_processed.parquet", taxi_type_column='taxi_type')
fhv_df = process_in_chunks_with_dask("taxi_data/processed/fhv_processed.parquet", taxi_type_column='taxi_type')
fhvhv_df = process_in_chunks_with_dask("taxi_data/processed/fhvhv_processed.parquet", taxi_type_column='taxi_type')

# Optionally, save the processed datasets with new features
yellow_df.to_parquet("taxi_data/processed/yellow_with_features.parquet", compression="snappy")
green_df.to_parquet("taxi_data/processed/green_with_features.parquet", compression="snappy")
fhv_df.to_parquet("taxi_data/processed/fhv_with_features.parquet", compression="snappy")
fhvhv_df.to_parquet("taxi_data/processed/fhvhv_with_features.parquet", compression="snappy")

print("Feature engineering completed and saved!")
