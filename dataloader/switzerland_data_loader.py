import pandas as pd
import os
from datetime import datetime
from pymongo import MongoClient
from starlette.responses import JSONResponse
from .update_db_records import update_records_in_database
from .load_data import fetch_file_from_url
from config import remote_urls, database_name, mongo_db_url


def update_switzerland_db(request):
    dataset_directory_path = os.getcwd() + os.sep + "data" + os.sep
    fetch_file_from_url(remote_urls['switzerland_data_confirmed_cases'],
                        'switzerland_data_confirmed' + '.csv',
                        directory_to_save=dataset_directory_path)
    fetch_file_from_url(remote_urls['switzerland_data_recovered_cases'],
                        'switzerland_data_recovered' + '.csv',
                        directory_to_save=dataset_directory_path)
    fetch_file_from_url(remote_urls['switzerland_data_death_cases'],
                        'switzerland_data_deaths' + '.csv',
                        directory_to_save=dataset_directory_path)

    dataset_directory_path1 = os.getcwd() + "/data/"
    df_confirmed = pd.read_csv(dataset_directory_path1 + 'switzerland_data_confirmed.csv')
    df_recovered = pd.read_csv(dataset_directory_path1 + 'switzerland_data_recovered.csv')
    df_death = pd.read_csv(dataset_directory_path1 + 'switzerland_data_deaths.csv')

    save_confirmed_cases(df_confirmed)
    save_recovered_cases(df_recovered)
    save_death_cases(df_death)

    return JSONResponse('files fetched', status_code=200)


def save_confirmed_cases(df_confirmed):
    df_confirmed["Date"] = pd.to_datetime(df_confirmed["Date"])
    df_confirmed.fillna(method='ffill', inplace=True)
    df_confirmed.fillna(0, inplace=True)

    date_list = df_confirmed['Date'].tolist()
    CH_list = df_confirmed['CH'].tolist()

    dict = {
        'viz_type': 'confirmed_data_day_wise_list',
        'Dates': [(datetime.strptime(str(date_string), "%Y-%m-%d %H:%M:%S")).timestamp() for
                  date_string in date_list],
        'confirmed': CH_list,
        'totalConfirmedCases': int(df_confirmed["CH"].iloc[-1]),
        'created_at': datetime.timestamp(datetime.now())
    }
    df_confirmed1 = df_confirmed.drop(columns=['Date'])
    columns = list(df_confirmed1)

    kanton_details_list = []
    for column in columns:
        kanton_data_list = df_confirmed1[column].tolist()
        dict1 = {
            'kanton': column,
            'confirmed': kanton_data_list
        }
        kanton_details_list.append(dict1)

    dictionary = {
        'viz_type': 'confirmed_cases_per_kanton_day_wise',
        'kanton_details': kanton_details_list,
        'dates': [(datetime.strptime(str(date_string), "%Y-%m-%d %H:%M:%S")).timestamp() for
                  date_string in date_list],
        'created_at': datetime.timestamp(datetime.now())
    }

    update_records_in_database('switzerland_data', dict, viz_type='confirmed_data_day_wise_list')
    update_records_in_database('switzerland_data', dictionary, viz_type='confirmed_cases_per_kanton_day_wise')
    return 0


def save_recovered_cases(df_recovered):
    df_recovered["Date"] = pd.to_datetime(df_recovered["Date"])
    df_recovered.fillna(method='ffill', inplace=True)
    df_recovered.fillna(0, inplace=True)

    date_list = df_recovered['Date'].tolist()
    CH_list = df_recovered['CH'].tolist()

    dict = {
        'viz_type': 'recovered_data_day_wise_list',
        'Dates': [(datetime.strptime(str(date_string), "%Y-%m-%d %H:%M:%S")).timestamp() for
                  date_string in date_list],
        'recovered': CH_list,
        'totalRecoveredCases': int(df_recovered["CH"].iloc[-1]),
        'created_at': datetime.timestamp(datetime.now())
    }

    df_recovered1 = df_recovered.drop(columns=['Date'])
    columns = list(df_recovered1)

    kanton_details_list = []
    for column in columns:
        kanton_data_list = df_recovered1[column].tolist()
        dict1 = {
            'kanton': column,
            'recovered': kanton_data_list
        }
        kanton_details_list.append(dict1)

    dictionary = {
        'viz_type': 'recovered_cases_per_kanton_day_wise',
        'kanton_details': kanton_details_list,
        'dates': [(datetime.strptime(str(date_string), "%Y-%m-%d %H:%M:%S")).timestamp() for
                  date_string in date_list],
        'created_at': datetime.timestamp(datetime.now())
    }

    update_records_in_database('switzerland_data', dict, viz_type='recovered_data_day_wise_list')
    update_records_in_database('switzerland_data', dictionary, viz_type='recovered_cases_per_kanton_day_wise')

    return 0


def save_death_cases(df_death):
    df_death["Date"] = pd.to_datetime(df_death["Date"])
    df_death.fillna(method='ffill', inplace=True)
    df_death.fillna(0, inplace=True)

    date_list = df_death['Date'].tolist()
    CH_list = df_death['CH'].tolist()

    dict = {
        'viz_type': 'death_data_day_wise_list',
        'Dates': [(datetime.strptime(str(date_string), "%Y-%m-%d %H:%M:%S")).timestamp() for
                  date_string in date_list],
        'death': CH_list,
        'totalDeathCases': int(df_death["CH"].iloc[-1]),
        'created_at': datetime.timestamp(datetime.now())
    }

    df_death1 = df_death.drop(columns=['Date'])
    columns = list(df_death1)

    kanton_details_list = []
    for column in columns:
        kanton_data_list = df_death1[column].tolist()
        dict1 = {
            'kanton': column,
            'death': kanton_data_list
        }
        kanton_details_list.append(dict1)

    dictionary = {
        'viz_type': 'death_cases_per_kanton_day_wise',
        'kanton_details': kanton_details_list,
        'dates': [(datetime.strptime(str(date_string), "%Y-%m-%d %H:%M:%S")).timestamp() for
                  date_string in date_list],
        'created_at': datetime.timestamp(datetime.now())
    }

    update_records_in_database('switzerland_data', dict, viz_type='death_data_day_wise_list')
    update_records_in_database('switzerland_data', dictionary, viz_type='death_cases_per_kanton_day_wise')

    return 0
