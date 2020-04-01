import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from config import database_name, mongo_db_url, country_code_dict
from .load_data import get_latest_time_series_file_dict


def update_map_data() -> int:

    file_list = get_latest_time_series_file_dict()

    country_wise_cases_dfs = {
        'confirmed': pd.read_csv(file_list['confirmed']),
        'recovered': pd.read_csv(file_list['recovered']),
        'deaths': pd.read_csv(file_list['deaths'])
    }

    country_wise_cases_dicts = {}

    for key, df in country_wise_cases_dfs.items():
        # because kingdom of Denmark is kingdom of denmark + greenland
        index_location = df.loc[df['Province/State'] == 'Greenland'].index
        df.iloc[index_location[0], 1] = 'Greenland'

        tmp_df = df.filter(['Country/Region'], axis=1)
        tmp_df[key] = df.iloc[:, -1]
        tmp_df = tmp_df.groupby(["Country/Region"]).agg({key: 'sum'}).reset_index()

        for index, row in tmp_df.iterrows():
            if row['Country/Region'] == 'Taiwan*':
                tmp_df.at[index, 'Country/Region'] = 'Taiwan'
            if row['Country/Region'] == 'US':
                tmp_df.at[index, 'Country/Region'] = 'United States'
            if row['Country/Region'] == 'Burma':
                tmp_df.at[index, 'Country/Region'] = 'Myanmar'
            if row['Country/Region'] == 'Congo (Brazzaville)':
                tmp_df.at[index, 'Country/Region'] = 'Republic of Congo'
            if row['Country/Region'] == 'Congo (Kinshasa)':
                tmp_df.at[index, 'Country/Region'] = 'Democratic Republic of the Congo'
            if row['Country/Region'] == 'Czechia':
                tmp_df.at[index, 'Country/Region'] = 'Czech Republic'
            if row['Country/Region'] == 'Eswatini':
                tmp_df.at[index, 'Country/Region'] = 'Swaziland'
            if row['Country/Region'] == 'West Bank and Gaza':
                tmp_df.at[index, 'Country/Region'] = 'Palestine'

            # tmp_df.to_csv('/home/schartz/' + key + '1.csv')

            country_code = country_code_dict[tmp_df.at[index, 'Country/Region']]
            tmp_df.at[index, 'country_code'] = country_code

        tmp_df.rename(columns={'Country/Region': 'Country_Region'}, inplace=True)
        country_wise_cases_dicts[key] = tmp_df.to_dict(orient='records')

    return update_records_in_database('visualizations', {
        'viz_type': 'map_global',
        'confirmed_list': country_wise_cases_dicts['confirmed'],
        'recovered_list': country_wise_cases_dicts['recovered'],
        'deaths_list': country_wise_cases_dicts['deaths'],

        'created_at': datetime.timestamp(datetime.now())
    })


def update_records_in_database(collection_name: str, dict_to_update: dict) -> int:
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db[collection_name]

        collection.remove({'viz_type': 'map_global'})

        collection.insert(dict_to_update)
    return 0
