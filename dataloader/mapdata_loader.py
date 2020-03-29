import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from config import remote_urls, dataset_directory_path, database_name, mongo_db_url
import numpy as np


def update_map_data(df: pd.DataFrame) -> int:
    collection_name = 'visualizations'
    df["ObservationDate"] = pd.to_datetime(df["ObservationDate"])
    df['Lat'].replace('', 0, inplace=True)
    df['Long'].replace('', 0, inplace=True)

    df_without_lat_long = df.head(2700)
    t = df_without_lat_long.groupby(["Country_Region", 'Province_State']).agg(
        {"Confirmed": np.sum, "Recovered": np.sum, "Deaths": np.sum}).reset_index()
    t.to_csv('/home/schartz/without_lat_long.csv')



    print(df_without_lat_long.head(1))
    print(df.tail(1))

    location_grouped_df = df.groupby(["Country_Region", 'Province_State']).agg(
        {"Confirmed": np.sum, "Recovered": np.sum, "Deaths": np.sum}).reset_index()

    location_grouped_df.to_csv('/home/schartz/g1.csv')

    map_data_df = df[
        ['Province_State', 'Country_Region', 'Confirmed', 'Recovered', 'Deaths', 'Lat', 'Long']
    ].copy()

    list_confirmed = []
    list_recovered = []
    list_deaths = []
    list_geo_coord = []
    for row in df.itertuples(index=True, name='Pandas'):
        list_confirmed.append({
            'name': getattr(row, "Province_State"),
            'value': getattr(row, 'Confirmed')
        })

        list_recovered.append({
            'name': getattr(row, "Province_State"),
            'value': getattr(row, 'Recovered')
        })
        list_deaths.append({
            'name': getattr(row, "Province_State"),
            'value': getattr(row, 'Deaths')
        })

        list_geo_coord.append({
            getattr(row, "Province_State").replace('.', ''): [getattr(row, "Lat"), getattr(row, 'Long')]
        })

    return update_records_in_database(collection_name, {
        'viz_type': 'map_global',
        'geo_cord_list': list_geo_coord,
        'confirmed_list': list_confirmed,
        'recovered_list': list_recovered,
        'death_list': list_deaths,
        'created_at': datetime.timestamp(datetime.now())
    })


def update_records_in_database(collection_name: str, dict_to_update: dict) -> int:
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db[collection_name]
        collection.insert(dict_to_update)
    return 0


