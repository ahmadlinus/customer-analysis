import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd
from load_data import load_stores_data


def load_general_area():
    '''
        loading the map for the area of interest, in this case the city of Berlin
    '''
    berlin_area_map_file = "berlin_map/bezirksgrenzen.shp"
    berlin_map = gpd.read_file(berlin_area_map_file)
    berlin_map = berlin_map.set_crs(epsg=4326)
    return berlin_map


def plot_users():
    '''
        visualize user GPS signals on a map of Berlin as background
    '''
    # loading and cleaning geo-spatial aspects
    user_visits_filepath = "derived_data/visits.csv.zip"
    area_map = load_general_area()
    visits = pd.read_csv(user_visits_filepath, compression="zip")
    visits.rename(columns={"geometry": "user_location"})
    visits = gpd.GeoDataFrame(visits, geometry=gpd.points_from_xy(visits.lon, visits.lat))
    visits = visits.set_crs(epsg=4326)

    # preparing the visualization
    _, ax = plt.subplots(figsize=(7, 7))
    area_map.to_crs(epsg=4326).plot(ax=ax, color="lightgrey")
    visits.plot(ax=ax)
    ax.set_title("Store Visits in Berlin")
    plt.show()


def plot_store_polygons():
    '''
        visualizing store polygons
    '''
    stores_filepath = "train_data/stores.csv"
    stores_gdf = load_stores_data(stores_filepath)
    stores_gdf = stores_gdf.set_crs(epsg=4326)
    area_map = load_general_area()

    # preparing the visualization
    _, ax = plt.subplots(figsize=(10, 10))
    area_map.to_crs(epsg=4326).plot(ax=ax, color="#FFFFE0")
    stores_gdf.plot(ax=ax, color="red", linewidth=5)
    ax.set_title("Store Locations in Berlin")
    plt.show()


if __name__ == "__main__":
    plot_users()
    plot_store_polygons()
