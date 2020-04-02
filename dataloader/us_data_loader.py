import pandas as pd
import os
import json
import re
import time
from bson import json_util
from datetime import datetime
from pymongo import MongoClient
from starlette.responses import JSONResponse
from .mapdata_loader import update_map_data
from config import remote_urls, dataset_directory_path, database_name, mongo_db_url


def update_us_db(request):
    dataset_directory_path = os.getcwd() + "/data/"
    dataset_file_list = os.listdir(dataset_directory_path)
    covid_us_df = pd.read_csv(dataset_directory_path + dataset_file_list[2])

    covid_us_df["date"] = pd.to_datetime(covid_us_df["date"])

    datewise_us_df = covid_us_df.groupby(["date"]).agg({"cases": 'sum', "deaths": 'sum'}).reset_index()
    print(datewise_us_df.info())

    date_list = datewise_us_df['date'].tolist()
    case_list = datewise_us_df['cases'].tolist()
    death_list = datewise_us_df['deaths'].tolist()

    dictionary = {
        'viz_type': 'us_data_daywise_visualization',
        'date': [(datetime.strptime(str(date_string), "%Y-%m-%d %H:%M:%S")).timestamp() for
                 date_string in date_list],
        'confirmed': case_list,
        'deaths': death_list
    }

    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.visualizations
        collection.insert(dictionary)

    return JSONResponse('Data successfully saved to database', status_code=200)


def save_state_data(request):
    dataset_directory_path = os.getcwd() + "/data/"
    dataset_file_list = os.listdir(dataset_directory_path)
    covid_us_df = pd.read_csv(dataset_directory_path + dataset_file_list[2])

    covid_us_df["date"] = pd.to_datetime(covid_us_df["date"])
    print(covid_us_df)
    state_list = covid_us_df['state'].unique()

    states_data_list = []
    for state in state_list:
        state_data = covid_us_df[covid_us_df['state'] == state]
        datewise_data = state_data.groupby(["date"]).agg({"cases": 'sum', "deaths": 'sum'}).reset_index()

        date_list = datewise_data['date'].tolist()
        case_list = datewise_data['cases'].tolist()
        death_list = datewise_data['deaths'].tolist()

        dict = {
            'name': state,
            'date_list': [(datetime.strptime(str(date_string), "%Y-%m-%d %H:%M:%S")).timestamp() for
                          date_string in date_list],
            'confirmed': case_list,
            'deaths': death_list
        }
        states_data_list.append(dict)

    dictionary = {
        'viz_type': 'state_data_visualization',
        'data': states_data_list,
        'created_at': datetime.timestamp(datetime.now())
    }

    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.visualizations
        collection.insert(dictionary)

    return JSONResponse('ue states data saved to mongo', status_code=200)


def total_cases_statewise(request):
    dataset_directory_path = os.getcwd() + "/data/"
    dataset_file_list = os.listdir(dataset_directory_path)
    covid_us_df = pd.read_csv(dataset_directory_path + dataset_file_list[2])

    covid_us_df["date"] = pd.to_datetime(covid_us_df["date"])
    print(covid_us_df)

    # state_data = covid_us_df[covid_us_df['state'] == 'Illinois']
    # datewise_data = state_data.groupby(["date"]).agg({"cases": 'sum', "deaths": 'sum'}).reset_index()
    # print(datewise_data["cases"].sum())
    # print(datewise_data["deaths"].sum())

    state_list = covid_us_df['state'].unique()

    states_data_list = []
    for state in state_list:
        state_data = covid_us_df[covid_us_df['state'] == state]
        datewise_data = state_data.groupby(["date"]).agg({"cases": 'sum', "deaths": 'sum'}).reset_index()

        dict = {
            'name': state,
            'confirmed': int(datewise_data["cases"].sum()),
            'deaths': int(datewise_data['deaths'].sum())
        }
        states_data_list.append(dict)

    dictionary = {
        'viz_type': 'total_cases_in_states',
        'data': states_data_list,
        'created_at': datetime.timestamp(datetime.now())

    }

    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.visualizations
        collection.insert(dictionary)

    return JSONResponse('data updated', status_code=200)


def state_visualization_bargraph(request):
    dataset_directory_path = os.getcwd() + "/data/"
    dataset_file_list = os.listdir(dataset_directory_path)
    covid_us_df = pd.read_csv(dataset_directory_path + dataset_file_list[2])

    covid_us_df["date"] = pd.to_datetime(covid_us_df["date"])
    print(covid_us_df)
    state_wise_df = covid_us_df.groupby(["state"]).agg({"cases": 'sum', "deaths": 'sum'}).reset_index()

    state_wise_plot = state_wise_df[state_wise_df["cases"] > 100].sort_values(["cases"]).head(54)
    state_wise_plot2 = state_wise_df[state_wise_df["cases"] > 100].sort_values(["deaths"]).head(54)


    dict = {
        'viz_type': 'states_case_visualization',
        'death_list': state_wise_plot2['deaths'].tolist(),
        'case_list': state_wise_plot['cases'].tolist(),
        'y_list': state_wise_df['state'].tolist()
    }

    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.visualizations
        collection.insert(dict)

    return JSONResponse('data updated', status_code=200)
