import logging
import os
import re
import time
from datetime import datetime
from pymongo import MongoClient

import pandas as pd
from starlette.responses import JSONResponse
from .mapdata_loader import update_map_data
from config import remote_urls, dataset_directory_path, database_name, mongo_db_url
from .load_data import get_latest_time_series_file_dict


async def update_db(request):
    if update_all_cases_cumulative_global_record() == 0 and \
            update_total_cases_global_record() == 0 and \
            update_country_wise_mortality_rate() == 0 and \
            update_data_for_table() == 0 and \
            update_map_data() == 0 and \
            update_basic_data_for_countries() == 0 and \
            update_country_wise_per_day_data() == 0:
        return JSONResponse({'message': 'Updated database with latest time series data'}, status_code=200)
    return JSONResponse({'message': 'Could not up date database with latest data'}, status_code=500)


def get_latest_extracted_dataset_directory() -> str:
    zip_file_list = []
    filename_regex = r'([a-zA-Z0-9\s_\\.\-\(\):])+(.zip)$'
    for item in os.listdir(dataset_directory_path):
        if re.match(filename_regex, item):
            zip_file_list.append(item)

    if len(zip_file_list) > 0:
        return dataset_directory_path + zip_file_list[-1].replace('zip', '') + os.sep + remote_urls[
            'daily_reports_relative_path']
    return 'na'


def get_sorted_dataset_latest_file_list(extracted_dataset_directory) -> list:
    filename_regex = r'^((0|1)\d{1})-((0|1|2)\d{1})-((19|20)\d{2}).csv'
    dataset_file_list = []

    for item in os.listdir(extracted_dataset_directory):
        if re.match(filename_regex, item):
            dataset_file_list.append(item)

    dataset_file_list.sort(key=lambda date: datetime.strptime(date.replace('.csv', ''), '%m-%d-%Y'))
    return dataset_file_list


def update_country_wise_per_day_data():
    file_list = get_latest_time_series_file_dict()

    time_series_dfs = {
        'confirmed': pd.read_csv(file_list['confirmed']),
        'recovered': pd.read_csv(file_list['recovered']),
        'deaths': pd.read_csv(file_list['deaths'])
    }

    result = 1

    for key, df in time_series_dfs.items():
        date_list = df.columns.values.tolist()
        del date_list[0:4]
        df.drop(['Province/State', 'Lat', 'Long'], axis=1, inplace=True)
        df = df.groupby(["Country/Region"]).agg(
            sum).reset_index()
        df_as_list = df.to_numpy().tolist()
        list_of_country_wise_dicts = []
        for individual_country_list in df_as_list:
            list_of_country_wise_dicts.append({
                'country': individual_country_list[0],
                key: individual_country_list[1:],
                'dates': [datetime.strptime(re.sub('/20$', '/2020', date_string), "%m/%d/%Y") for date_string in
                          date_list]
            })

        dict_to_save_in_mongo = {
            'viz_type': 'time_series_country_wise_' + key,
            'data': list_of_country_wise_dicts,
            'created_at': str(int(round(time.time() * 1000)))
        }

        result = update_records_in_database('visualizations', dict_to_save_in_mongo,
                                            dict_to_save_in_mongo['viz_type'])
    return result


def update_data_for_table() -> int:
    file_list = get_latest_time_series_file_dict()

    time_series_dfs = {
        'confirmed': pd.read_csv(file_list['confirmed']),
        'recovered': pd.read_csv(file_list['recovered']),
        'deaths': pd.read_csv(file_list['deaths'])
    }
    dict_to_save_in_mongo = {
        'viz_type': 'country_wise_table_data',
        'created_at': datetime.timestamp(datetime.now())
    }
    master_list = []
    for key, df in time_series_dfs.items():

        # because kingdom of Denmark is kingdom of denmark + greenland
        index_location = df.loc[df['Province/State'] == 'Greenland'].index
        df.iloc[index_location[0], 1] = 'Greenland'

        df.drop(['Province/State', 'Lat', 'Long'], axis=1, inplace=True)
        df = df.groupby(["Country/Region"]).agg(
            sum).reset_index()
        df_as_list = df.to_numpy().tolist()
        list_of_case_count_list = []
        for individual_country_list in df_as_list:
            list_of_case_count_list.append({
                'country': individual_country_list[0],
                key: individual_country_list[-1]
            })

        master_list.append(list_of_case_count_list)

    final_list = []

    for i in range(len(master_list[0])):
        if master_list[0][i]['confirmed'] > 0:
            final_list.append({
                'country': master_list[0][i]['country'],
                'confirmed': master_list[0][i]['confirmed'],
                'recovered': master_list[1][i]['recovered'],
                'deaths': master_list[2][i]['deaths'],
                'mortality_rate': (master_list[2][i]['deaths']) / (master_list[0][i]['confirmed']),
                'recovery_rate': (master_list[1][i]['recovered']) / (master_list[0][i]['confirmed']),
            })

    dict_to_save_in_mongo['data'] = final_list
    return update_records_in_database('visualizations', dict_to_save_in_mongo,
                                      dict_to_save_in_mongo['viz_type'])


def update_basic_data_for_countries() -> int:
    if df_type == 'time_series':
        return 0
    countries_basic_data = get_all_countries_basic_data(df)

    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.country_wise_data
        collection.insert_many(countries_basic_data)
    return 0


def update_all_cases_cumulative_global_record() -> int:
    file_list = get_latest_time_series_file_dict()

    time_series_dfs = {
        'confirmed': pd.read_csv(file_list['confirmed']),
        'recovered': pd.read_csv(file_list['recovered']),
        'deaths': pd.read_csv(file_list['deaths'])
    }
    cases_list = {}
    dates_list = []
    for key, df in time_series_dfs.items():
        d = df.columns.values.tolist()
        del d[0:4]
        dates_list = [(datetime.strptime(re.sub('/20$', '/2020', date_string), "%m/%d/%Y")).timestamp() for
                      date_string in d]
        df.drop(['Country/Region', 'Province/State', 'Lat', 'Long'], axis=1, inplace=True)
        cases_list[key] = df.sum(axis=0).tolist()

    dict_to_save_in_mongo = {
        'viz_type': 'all_cases_cumulative_global',
        'dates': dates_list,
        'confirmed': cases_list['confirmed'],
        'recovered': cases_list['recovered'],
        'deaths': cases_list['deaths'],
        'created_at': datetime.timestamp(datetime.now())
    }

    return update_records_in_database('visualizations', dict_to_save_in_mongo, dict_to_save_in_mongo['viz_type'])


def update_total_cases_global_record() -> int:
    file_list = get_latest_time_series_file_dict()

    time_series_dfs = {
        'confirmed': pd.read_csv(file_list['confirmed']),
        'recovered': pd.read_csv(file_list['recovered']),
        'deaths': pd.read_csv(file_list['deaths'])
    }
    dict_to_save_in_mongo = {
        'viz_type': 'total_cases_global',
        'created_at': datetime.timestamp(datetime.now())
    }
    for key, df in time_series_dfs.items():
        total_countries_affected = len(df["Country/Region"].unique())

        dict_to_save_in_mongo['totalCountries'] = total_countries_affected
        dict_to_save_in_mongo[key] = int((df.iloc[:, -1]).sum())

    return update_records_in_database('visualizations', dict_to_save_in_mongo,
                                      dict_to_save_in_mongo['viz_type'])


def update_country_wise_mortality_rate() -> int:
    file_list = get_latest_time_series_file_dict()

    time_series_dfs = {
        'confirmed': pd.read_csv(file_list['confirmed']),
        'recovered': pd.read_csv(file_list['recovered']),
        'deaths': pd.read_csv(file_list['deaths'])
    }

    tmp = {
        'confirmed': pd.DataFrame(),
        'recovered': pd.DataFrame(),
        'deaths': pd.DataFrame()
    }
    for key, df in time_series_dfs.items():
        # because kingdom of Denmark is kingdom of denmark + greenland
        index_location = df.loc[df['Province/State'] == 'Greenland'].index
        df.iloc[index_location[0], 1] = 'Greenland'

        df.drop(['Province/State', 'Lat', 'Long'], axis=1, inplace=True)
        k = df.groupby(["Country/Region"]).agg(
            sum).reset_index()
        tmp[key] = k

    final_df = pd.DataFrame()
    final_df['country'] = tmp['confirmed']['Country/Region']
    final_df['confirmed'] = tmp['confirmed'].iloc[:, -1]
    final_df['recovered'] = tmp['recovered'].iloc[:, -1]
    final_df['deaths'] = tmp['deaths'].iloc[:, -1]
    final_df['mortality_rate'] = final_df['deaths'] / final_df['confirmed']
    final_df['recovery_rate'] = final_df['recovered'] / final_df['confirmed']
    final_df.sort_values(by=['confirmed'], ascending=False, inplace=True)

    dict_to_save_in_mongo = {
        'viz_type': 'country_wise_mortality_and_recovery_rates',
        'data': final_df.to_dict('list'),
        'created_at': datetime.timestamp(datetime.now())
    }
    return update_records_in_database('visualizations', dict_to_save_in_mongo,
                                      dict_to_save_in_mongo['viz_type'])


def get_all_countries_basic_data(df):
    countrywise_df = df.groupby(["Country_Region"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    countrywise_df["Country_Region"] = countrywise_df["Country_Region"].str.replace(' ', '')
    countrywise_df.drop_duplicates(subset="Country_Region", keep="last", inplace=True)
    countrywise_df['Country_Region'] = countrywise_df['Country_Region'].str.lower()

    countrywise_df["perCountryMortality"] = (countrywise_df["Deaths"] / countrywise_df["Confirmed"]) * 100

    country_wise_data_to_be_thrown_into_db = []
    for row_number in countrywise_df.index:
        tmp_dict = {
            "name": countrywise_df['Country_Region'][row_number],
            "basic": {
                "confirmed": countrywise_df['Confirmed'][row_number],
                "recovered": countrywise_df['Recovered'][row_number],
                "deaths": countrywise_df['Deaths'][row_number],
                "mortality": countrywise_df['perCountryMortality'][row_number]
            },
            "detailed": {},
            "created_at": datetime.timestamp(datetime.now())
        }
        country_wise_data_to_be_thrown_into_db.append(tmp_dict)

    return country_wise_data_to_be_thrown_into_db


def update_records_in_database(collection_name: str, dict_to_update: dict, viz_type: str) -> int:
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db[collection_name]

        collection.remove({'viz_type': viz_type})

        collection.insert(dict_to_update)
    return 0
