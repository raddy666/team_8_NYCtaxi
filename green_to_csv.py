import pandas as pd
for year in range(2010, 2013):  # 2014 and 2015
    for month in range(1, 13):  # 1 to 12
        filename = f"dataset/green/green_tripdata_{year}-{month:02d}.parquet"
        output = f"dataset/green_merge/green_tripdata_{year}-{month:02d}.csv"
        df = pd.read_parquet(filename, engine="pyarrow")
        # save to csv
        df.to_csv(output, index=False)
        print(filename)