import pandas as pd
import geopandas as gpd


def load_user_visit_data_chunk(path: str):
    df = pd.read_csv(path, compression="gzip")
    df["date"] = pd.to_datetime(df["utc_timestamp"], unit="ms").dt.date
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
    return gdf


def load_stores_data(path: str):
    gdf = gpd.read_file(path)
    return gdf


def load_affinity(path: str):
    df = pd.read_csv(path, compression="gzip")
    return df