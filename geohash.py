import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point

# Load cleaned data
df = pd.read_csv("processed_data/processed_data.csv")

# Convert coordinates to GeoDataFrame
df["Start_Point"] = df.apply(lambda row: Point(row["Start_Lon"], row["Start_Lat"]), axis=1)
df["End_Point"] = df.apply(lambda row: Point(row["End_Lon"], row["End_Lat"]), axis=1)

gdf_start = gpd.GeoDataFrame(df, geometry="Start_Point", crs="EPSG:4326")
gdf_end = gpd.GeoDataFrame(df, geometry="End_Point", crs="EPSG:4326")

# Load NYC neighborhood map
try:
    nyc_map = gpd.read_file("dataset/nyc_neighborhoods.geojson")
    if nyc_map.empty:
        raise ValueError("NYC map file is empty.")
    nyc_map = nyc_map.to_crs(epsg=4326)  # Ensure correct projection
except Exception as e:
    print(f"Error loading map: {e}")
    exit()

# Plot data
fig, ax = plt.subplots(figsize=(10, 10))
nyc_map.plot(ax=ax, color="lightgray", edgecolor="black", alpha=0.5)
gdf_start.plot(ax=ax, color="blue", markersize=5, alpha=0.5)
gdf_end.plot(ax=ax, color="red", markersize=5, alpha=0.5)

# Adjust map bounds for NYC area
ax.set_xlim([-74.3, -73.7])  # Longitude range for NYC
ax.set_ylim([40.5, 41.0])    # Latitude range for NYC

plt.title("NYC Taxi Trip Locations", fontsize=14)
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.show()
