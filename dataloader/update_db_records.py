import logging
import os
import re
from datetime import datetime
from pymongo import MongoClient

import pandas as pd
import numpy as np
from starlette.responses import JSONResponse

from config import remote_urls, dataset_directory_path, database_name, mongo_db_url


async def update_db(request):
    if update_cumulative_cases_record() == 0:
        return JSONResponse({'message': 'Updated database with latest data'}, status_code=200)

    return JSONResponse({'message': 'Could not up date database with latest data'}, status_code=500)


def get_latest_extracted_dataset_directory():
    zip_file_list = []
    filename_regex = r'([a-zA-Z0-9\s_\\.\-\(\):])+(.zip)$'
    for item in os.listdir(dataset_directory_path):
        if re.match(filename_regex, item):
            zip_file_list.append(item)

    return dataset_directory_path + zip_file_list[-1].replace('zip', '') + os.sep + remote_urls[
        'daily_reports_relative_path']


def get_sorted_dataset_latest_file_list(extracted_dataset_directory) -> list:
    filename_regex = r'^((0|1)\d{1})-((0|1|2)\d{1})-((19|20)\d{2}).csv'
    dataset_file_list = []

    for item in os.listdir(extracted_dataset_directory):
        if re.match(filename_regex, item):
            dataset_file_list.append(item)

    dataset_file_list.sort(key=lambda date: datetime.strptime(date.replace('.csv', ''), '%m-%d-%Y'))
    return dataset_file_list


def update_cumulative_cases_record() -> int:
    extracted_dataset_directory = get_latest_extracted_dataset_directory()
    mini_data_frame_list = []

    logging.info('Reading data from the latest directory: ' + extracted_dataset_directory)
    for item in get_sorted_dataset_latest_file_list(extracted_dataset_directory):
        mini_data_frame_list.append(
            pre_process_data_frame_for_different_columns(
                pd.read_csv(extracted_dataset_directory + os.sep + item), item))

    final_data_frame = pd.concat(mini_data_frame_list)
    cumulative_cases_global = get_visualization_ready_dict(final_data_frame)

    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.visualizations
        collection.insert(cumulative_cases_global)

    return 0


def pre_process_data_frame_for_different_columns(df: pd.DataFrame, item) -> pd.DataFrame:
    if 'FIPS' in df.columns:
        df.drop('FIPS', inplace=True, axis=1)
    if 'Active' in df.columns:
        df.drop('Active', inplace=True, axis=1)
    if 'Combined_Key' in df.columns:
        df.drop('Combined_Key', inplace=True, axis=1)
    if 'Admin2' in df.columns:
        df.drop('Admin2', inplace=True, axis=1)
    if 'Province/State' in df.columns:
        df.rename(columns={"Province/State": "Province_State"}, inplace=True)
    if 'Country/Region' in df.columns:
        df.rename(columns={"Country/Region": "Country_Region"}, inplace=True)
    if 'Longitude' in df.columns:
        df.rename(columns={"Longitude": "Long"}, inplace=True)
    if 'Latitude' in df.columns:
        df.rename(columns={"Latitude": "Lat"}, inplace=True)
    if 'Long_' in df.columns:
        df.rename(columns={"Long_": "Long"}, inplace=True)
    if 'Last Update' in df.columns:
        df.rename(columns={"Last Update": "Last_Update"}, inplace=True)

    pd.to_datetime(df["Last_Update"])
    df['ObservationDate'] = item.replace('-', '/').replace('.csv', '').strip()

    return df


def get_visualization_ready_dict(df: pd.DataFrame) -> dict:
    df.reset_index(drop=True, inplace=True)
    df.fillna('', inplace=True)
    df['Confirmed'].replace('', 0, inplace=True)
    df['Recovered'].replace('', 0, inplace=True)
    df['Deaths'].replace('', 0, inplace=True)

    df["ObservationDate"] = pd.to_datetime(df["ObservationDate"])
    date_wise_df = df.groupby(["ObservationDate"]).agg(
            {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()


    ##########################All cases combined cumulative##########################
    arr_yax = date_wise_df['Confirmed'].to_numpy()
    print(arr_yax)
    y_list = arr_yax.tolist()

    arr_xax = date_wise_df.ObservationDate.to_numpy()
    x_list = arr_xax.tolist()

    arr_recovered = date_wise_df['Recovered'].to_numpy()
    arr_deaths = date_wise_df['Deaths'].to_numpy()
    recovered_list = arr_recovered.tolist()
    death_list = arr_deaths.tolist()

    dictionary_cumulative = {
        'viz_type': 'all_cases_cumulative_global',
        'json_xax': x_list,
        'confirmed': y_list,
        'recovered': recovered_list,
        'death': death_list,
        'created_at': datetime.timestamp(datetime.now())
    }
    ##########################All cases combined cumulative end##########################

    ##########################All cases combined Total##########################
    # extract the information from the dataset
    total_countries_affected = len(df["Country_Region"].unique())
    total_confirmed_cases = date_wise_df["Confirmed"].iloc[-1]
    total_recovered_cases = date_wise_df["Recovered"].iloc[-1]
    total_deaths = date_wise_df["Deaths"].iloc[-1]

    dictionary_total = {
        'viz_type': 'total_cases_global',
        'totalCountries': total_countries_affected,
        'confirmed': total_confirmed_cases,
        'recovered': total_recovered_cases,
        'deaths': total_deaths,
        'created_at': datetime.timestamp(datetime.now())
    }
    ##########################All cases combined Total Ends##########################

    return dictionary_cumulative
