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
    if request.path_params['file_type'] not in ['time-series', 'combined']:
        message = request.path_params['file_type'] + \
                  ' does not match any known operations. Available operations are \'time-series\' and \'combined\''
        return JSONResponse({'message': message}, status_code=500)

    if request.path_params['file_type'] == 'time-series':
        if update_all_cases_cumulative_global_record(pd.DataFrame(), df_type='time_series') == 0 and \
                update_total_cases_global_record(pd.DataFrame(), df_type='time_series') == 0 and \
                update_country_wise_mortality_rate(pd.DataFrame(), df_type='time_series') == 0 and \
                update_data_for_table(pd.DataFrame(), df_type='time_series') == 0 and \
                update_map_data() == 0 and \
                update_basic_data_for_countries(pd.DataFrame(), df_type='time_series') == 0 and \
                update_country_wise_per_day_data(pd.DataFrame(), df_type='time_series') == 0:
            return JSONResponse({'message': 'Updated database with latest time series data'}, status_code=200)
        return JSONResponse({'message': 'Could not up date database with latest data'}, status_code=500)

    else:
        extracted_dataset_directory = get_latest_extracted_dataset_directory()
        if extracted_dataset_directory != 'na':
            mini_data_frame_list = []
            logging.info('Reading data from the latest directory: ' + extracted_dataset_directory)
            for item in get_sorted_dataset_latest_file_list(extracted_dataset_directory):
                mini_data_frame_list.append(
                    pre_process_data_frame_for_different_columns(
                        pd.read_csv(extracted_dataset_directory + os.sep + item), item))

            final_data_frame = pd.concat(mini_data_frame_list)
            final_data_frame.reset_index(drop=True, inplace=True)
            final_data_frame.fillna('', inplace=True)
            final_data_frame['Confirmed'].replace('', 0, inplace=True)
            final_data_frame['Recovered'].replace('', 0, inplace=True)
            final_data_frame['Deaths'].replace('', 0, inplace=True)

            if update_all_cases_cumulative_global_record(final_data_frame, df_type='combined') == 0 and \
                    update_total_cases_global_record(final_data_frame, df_type='combined') == 0 and \
                    update_country_wise_mortality_rate(final_data_frame, df_type='combined') == 0 and \
                    update_data_for_table(final_data_frame, df_type='combined') == 0 and \
                    update_map_data() == 0 and \
                    update_basic_data_for_countries(final_data_frame, df_type='combined') == 0 and \
                    update_country_wise_per_day_data(final_data_frame, df_type='combined') == 0:
                return JSONResponse({'message': 'Updated database with latest data'}, status_code=200)
            return JSONResponse({'message': 'Could not update database with latest data'}, status_code=500)
        else:
            return JSONResponse({'message': 'Could not find an extracted dataset'}, status_code=500)


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


def update_country_wise_per_day_data(df: pd.DataFrame, df_type: str):
    if df_type == 'combined':
        countries_per_day_data = get_all_countries_per_day_dict(df)
        return update_records_in_database('visualizations', countries_per_day_data, countries_per_day_data['viz_type'])
    else:
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


def update_data_for_table(df: pd.DataFrame, df_type: str) -> int:
    if df_type == 'time_series':
        return 0
    countries_table_data = get_all_countries_table_data_ready_dict(df)

    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.countries_table
        collection.insert(countries_table_data)
    return 0


def update_basic_data_for_countries(df: pd.DataFrame, df_type: str) -> int:
    if df_type == 'time_series':
        return 0
    countries_basic_data = get_all_countries_basic_data(df)

    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.country_wise_data
        collection.insert_many(countries_basic_data)
    return 0


def update_all_cases_cumulative_global_record(df: pd.DataFrame, df_type: str) -> int:
    if df_type == 'combined':
        cumulative_cases_global = get_all_cases_cumulative_global_visualization_ready_dict(df)
        return update_records_in_database('visualizations', cumulative_cases_global,
                                          cumulative_cases_global['viz_type'])
    else:
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


def update_total_cases_global_record(df: pd.DataFrame, df_type: str) -> int:
    if df_type == 'combined':
        total_cases_global = get_total_cases_global_visualization_ready_dict(df)
        return update_records_in_database('visualizations', total_cases_global,
                                          total_cases_global['viz_type'])
    else:
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


def update_country_wise_mortality_rate(df: pd.DataFrame, df_type: str) -> int:
    if df_type == 'time_series':
        return 0
    cumulative_cases_global = get_country_wise_mortality_rate_visualization_ready_dict(df)

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


def get_all_countries_table_data_ready_dict(df) -> dict:
    countrywise_df = df.groupby(["Country_Region"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    countrywise_df["Country_Region"] = countrywise_df["Country_Region"].str.replace(' ', '')
    countrywise_df.drop_duplicates(subset="Country_Region", keep="last", inplace=True)
    countrywise_df['Country_Region'] = countrywise_df['Country_Region'].str.lower()
    country_records = countrywise_df.to_dict(orient='records')

    return country_records


def get_all_cases_cumulative_global_visualization_ready_dict(df: pd.DataFrame) -> dict:
    df["ObservationDate"] = pd.to_datetime(df["ObservationDate"])
    date_wise_df = df.groupby(["ObservationDate"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

    ##########################All cases combined cumulative##########################
    arr_yax = date_wise_df['Confirmed'].to_numpy()
    y_list = arr_yax.tolist()

    arr_xax = date_wise_df.ObservationDate.to_numpy()
    x_list = arr_xax.tolist()

    arr_recovered = date_wise_df['Recovered'].to_numpy()
    arr_deaths = date_wise_df['Deaths'].to_numpy()
    recovered_list = arr_recovered.tolist()
    death_list = arr_deaths.tolist()

    return {
        'viz_type': 'all_cases_cumulative_global',
        'json_xax': x_list,
        'confirmed': y_list,
        'recovered': recovered_list,
        'death': death_list,
        'created_at': datetime.timestamp(datetime.now())
    }


def get_all_countries_per_day_dict(final_data_frame):
    final_data_frame["ObservationDate"] = pd.to_datetime(final_data_frame["ObservationDate"])
    final_data_frame["Country_Region"] = final_data_frame["Country_Region"].str.lstrip()
    final_data_frame["Country_Region"] = final_data_frame["Country_Region"].str.replace(' ', '_')
    final_data_frame['Country_Region'] = final_data_frame['Country_Region'].str.lower()

    countries_list = final_data_frame['Country_Region'].unique()

    list_of_country_wise_dict = []

    for country in countries_list:
        country_new_data_frame = final_data_frame[final_data_frame["Country_Region"] == country]
        datewise_country_new_data_frame = country_new_data_frame.groupby(["ObservationDate"]).agg(
            {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

        date_list = datewise_country_new_data_frame['ObservationDate'].to_numpy().tolist()
        confirmed_list = datewise_country_new_data_frame['Confirmed'].to_numpy().tolist()
        recovered_list = datewise_country_new_data_frame['Recovered'].to_numpy().tolist()
        death_list = datewise_country_new_data_frame['Deaths'].to_numpy().tolist()

        temp_country_dict = {
            'name': country,
            'json_xax': date_list,
            'confirmed': confirmed_list,
            'recovered': recovered_list,
            'death': death_list,
        }
        list_of_country_wise_dict.append(temp_country_dict)

    print(len(list_of_country_wise_dict))

    return {
        "viz_type": "country_wise_dict",
        "data": list_of_country_wise_dict,
        "created_at": datetime.timestamp(datetime.now())
    }


def get_total_cases_global_visualization_ready_dict(df: pd.DataFrame) -> dict:
    df["ObservationDate"] = pd.to_datetime(df["ObservationDate"])
    date_wise_df = df.groupby(["ObservationDate"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    # extract the information from the dataset
    total_countries_affected = len(df["Country_Region"].unique())
    total_confirmed_cases = date_wise_df["Confirmed"].iloc[-1]
    total_recovered_cases = date_wise_df["Recovered"].iloc[-1]
    total_deaths = date_wise_df["Deaths"].iloc[-1]

    return {
        'viz_type': 'total_cases_global',
        'totalCountries': total_countries_affected,
        'confirmed': total_confirmed_cases,
        'recovered': total_recovered_cases,
        'deaths': total_deaths,
        'created_at': datetime.timestamp(datetime.now())
    }


def get_country_wise_mortality_rate_visualization_ready_dict(df: pd.DataFrame) -> dict:
    country_wise_df = df.groupby(["Country_Region"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

    country_wise_df["Mortality"] = (country_wise_df["Deaths"] / country_wise_df["Confirmed"]) * 100

    country_wise_plot_mortal = country_wise_df[country_wise_df["Confirmed"] > 50].sort_values(["Mortality"],
                                                                                              ascending=False).head(25)
    arr_yax = country_wise_plot_mortal['Country_Region'].to_numpy()
    y_list = arr_yax.tolist()

    arr_xax = country_wise_plot_mortal['Mortality'].to_numpy()
    x_list = arr_xax.tolist()

    return {
        'viz_type': 'country_wise_mortality',
        'json_xax': x_list,
        'json_yax': y_list
    }


def update_records_in_database(collection_name: str, dict_to_update: dict, viz_type: str) -> int:
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db[collection_name]

        collection.remove({'viz_type': viz_type})

        collection.insert(dict_to_update)
    return 0
