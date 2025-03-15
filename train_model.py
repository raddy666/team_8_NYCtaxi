import dask.dataframe as dd
import pandas as pd

def feature_engineering(df, taxi_type_column=None):
    # ensure datetime format
    df['pickup_time'] = pd.to_datetime(df['pickup_time'], errors='coerce')
    df['dropoff_time'] = pd.to_datetime(df['dropoff_time'], errors='coerce')

    df = df.dropna(subset=['pickup_time', 'dropoff_time'])

    # day
    df['pickup_day_of_week'] = df['pickup_time'].dt.dayofweek

    # hour
    df['pickup_hour'] = df['pickup_time'].dt.hour

    # trip duration
    df['trip_duration'] = (df['dropoff_time'] - df['pickup_time']).dt.total_seconds() / 60.0

    if taxi_type_column and taxi_type_column in df.columns:
        df = pd.get_dummies(df, columns=[taxi_type_column], drop_first=True)

    return df

def process_in_chunks_with_dask(file_path, taxi_type_column=None, columns=None):
    # Load the dataset metadata to check which columns exist
    ddf = dd.read_parquet(file_path, engine='pyarrow', columns=columns)

    ddf = ddf.map_partitions(feature_engineering, taxi_type_column)
    ddf = ddf.persist()

    return ddf.compute()

def train_model():
    columns = ['pickup_time', 'dropoff_time', 'taxi_type']  

    yellow_df = process_in_chunks_with_dask(
        "taxi_data/processed/yellow_processed.parquet", 
        taxi_type_column='taxi_type', 
        columns=columns
    )
    green_df = process_in_chunks_with_dask(
        "taxi_data/processed/green_processed.parquet", 
        taxi_type_column='taxi_type', 
        columns=columns
    )
    fhv_df = process_in_chunks_with_dask(
        "taxi_data/processed/fhv_processed.parquet", 
        taxi_type_column='taxi_type', 
        columns=columns
    )
    fhvhv_df = process_in_chunks_with_dask(
        "taxi_data/processed/fhvhv_processed.parquet", 
        taxi_type_column='taxi_type', 
        columns=columns
    )

    # Combine all
    df = pd.concat([yellow_df, green_df, fhv_df, fhvhv_df], ignore_index=True)

    if 'taxi_type' not in df.columns:
        print("taxi_type column not found, removing from the training data.")
        X = df[['pickup_day_of_week', 'pickup_hour']]
    else:
        X = df[['pickup_day_of_week', 'pickup_hour', 'taxi_type']] 

    y = df['trip_duration']

    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    from sklearn.ensemble import RandomForestRegressor
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    from sklearn.metrics import mean_squared_error
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    print(f'Mean Squared Error (MSE): {mse}')

    import matplotlib.pyplot as plt
    plt.figure(figsize=(8, 6))
    plt.scatter(y_test, y_pred, color='blue', alpha=0.5)
    plt.plot([0, max(y_test)], [0, max(y_test)], color='red', linestyle='--')  # Ideal line (y=x)
    plt.xlabel("Actual Trip Duration")
    plt.ylabel("Predicted Trip Duration")
    plt.title("Actual vs Predicted Trip Duration")
    plt.show()

train_model()
