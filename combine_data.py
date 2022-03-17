import time
import os
import pandas as pd
import geopandas as gpd
import multiprocessing as mp
from load_data import load_user_visit_data_chunk, load_stores_data, load_affinity



def combine_store_with_user_df(data_chunk_path):
    '''
        finding intersections between user data and store locations: who was in which store?
        perform a spatial join accelerated by geo-spatial indexing features of geopandas
    '''
    stores_data_path = "train_data/stores.csv"
    stores = load_stores_data(stores_data_path)
    print(data_chunk_path)
    gdf = load_user_visit_data_chunk(data_chunk_path)
    # right join to find the intersections between stores and user locations
    all_users = stores.sjoin(gdf, how="right")

    # non-visitors are those who haven't been in any store and their left index is null
    non_visitors = all_users[all_users["index_left"].isna()]
    visits = all_users[all_users["index_left"].notna()]
    return (non_visitors, visits)


def combine_store_location_visit_data():
    '''
        performing the intersection between user data and store locations in parallel 
        for all data chunks
    '''
    # empty data frames which will be filled with visitors and non-visitors
    all_visits = gpd.GeoDataFrame()
    no_visit = pd.DataFrame()
      
    # retreiving a list of gzip archives which are located in that particular directory
    user_data_dir = "train_data/full_data"
    files = os.listdir(user_data_dir) 
    gzip_files = [os.path.join(user_data_dir, f) for f in files if f.split(".")[-1] == "gz"]

    # performing spatial join operations in parallel
    num_procs = 8
    pool = mp.Pool(processes=num_procs)
    results = pool.map(combine_store_with_user_df, gzip_files)
    pool.close()

    # concatenating the results from all spatial joins
    for res in results:
        non_visitors, visits = res
        all_visits = pd.concat([all_visits, visits])
        no_visit = pd.concat([no_visit, non_visitors])

    # TODO: perform a left join between non visitors and visiors to get set difference (A - B)

    # saving to file
    all_visits.to_csv("derived_data/visits.csv", index=False)
    no_visit.to_csv("derived_data/non_visitors.csv", index=False)


def aggregate_combined_data():
    '''
        grouping by device ID and counting the total/unique visits for each store on a given date
    '''
    visits = pd.read_csv("visits.csv")
    grouped_visits = visits.groupby(["date", "store_name", "store_id"]).agg({"device_id": ["count", "unique"]})
    return grouped_visits


def create_user_affinity_profile():
    '''
        unify user affinities by loading all affinity classes and merging them
        into a single data frame
    '''
    affinities_dir = "train_data/affinities"
    gzip_files = [f for f in os.listdir(affinities_dir) if f.split(".")[-1] == "gz"]
    affinity_categories = [f.split(".")[0] for f in gzip_files]
    df_affinity_columns = ["device_id"] + affinity_categories
    df_affinity = pd.DataFrame(columns=df_affinity_columns)
    
    for affinity_file in gzip_files:
        affinity_file_full_path = os.path.join(affinities_dir, affinity_file)
        category = affinity_file.split(".")[0]
        print(category)
        df = load_affinity(affinity_file_full_path)
        df[category] = 1
        df_affinity = pd.concat([df_affinity, df])

    df_affinity = df_affinity.replace([pd.NA], 0)
    aggregation_dict = {affinity_category:'sum' for affinity_category in affinity_categories}
    df_affinity = df_affinity.groupby("device_id").agg(aggregation_dict).reset_index()
    df_affinity.to_csv("derived_data/affinity_matrix.csv", index=False)    


def combine_aggregated_data_with_affinity_data():
    '''
        make the ultimate goal: the dataframe like "example.csv" by combining affinity and location data
    '''
    pass


if __name__ == "__main__":
    t1 = time.time()
    combine_store_location_visit_data()
    t2 = time.time()
    print(t2 - t1)
    create_user_affinity_profile()