import pandas as pd
import os
from datetime import datetime
from pymongo import MongoClient
from starlette.responses import JSONResponse
from .update_db_records import update_records_in_database
from .load_data import fetch_file_from_url
from config import remote_urls, database_name, mongo_db_url


def update_us_db(request):
    dataset_directory_path = os.getcwd() + os.sep + "data" + os.sep
    fetch_file_from_url(remote_urls['us_states_data'],
                        'us_states_data' + '.csv',
                        directory_to_save=dataset_directory_path)

    dataset_directory_path1 = os.getcwd() + "/data/"
    covid_us_df = pd.read_csv(dataset_directory_path1 + 'us_states_data.csv')

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
        'deaths': death_list,
        'totalConfirmedCases': int(datewise_us_df["cases"].iloc[-1]),
        'totalDeathCases': int(datewise_us_df["deaths"].iloc[-1])
    }

    update_records_in_database('us_data', dictionary, viz_type='us_data_daywise_visualization')
    save_state_data(covid_us_df)
    total_cases_statewise(covid_us_df)
    state_visualization_bargraph(covid_us_df)

    return JSONResponse('Data successfully saved to database', status_code=200)


def save_state_data(covid_us_df):
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
        collection = db.us_data
        collection.insert(dictionary)

    return 0


def total_cases_statewise(covid_us_df):
    covid_us_df["date"] = pd.to_datetime(covid_us_df["date"])
    state_list = covid_us_df['state'].unique()

    states_data_list = []
    for state in state_list:
        state_data = covid_us_df[covid_us_df['state'] == state]
        state_data2 = state_data.iloc[[-1]]

        dict = {
            'name': state,
            'confirmed': state_data2['cases'].tolist(),
            'deaths': state_data2['deaths'].tolist()
        }
        states_data_list.append(dict)

    dictionary = {
        'viz_type': 'total_cases_in_states',
        'data': states_data_list,
        'created_at': datetime.timestamp(datetime.now())

    }

    update_records_in_database('us_data', dictionary, viz_type='total_cases_in_states')

    return 0


def state_visualization_bargraph(covid_us_df):
    covid_us_df["date"] = pd.to_datetime(covid_us_df["date"])

    state_wise_df = covid_us_df.groupby('state').agg({'cases': 'max', 'deaths': 'max'}).reset_index()

    dict = {
        'viz_type': 'states_case_visualization',
        'death_list': state_wise_df['deaths'].tolist(),
        'case_list': state_wise_df['cases'].tolist(),
        'y_list': state_wise_df['state'].tolist()
    }

    update_records_in_database('us_data', dict, viz_type='states_case_visualization')

    return 0
