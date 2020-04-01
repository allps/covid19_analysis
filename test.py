from dataloader import get_latest_time_series_file_dict
from datetime import datetime
import time, re
import pandas as pd


def test():
    # some

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
        final_list.append({
            'country': master_list[0][i]['country'],
            'confirmed': master_list[0][i]['confirmed'],
            'recovered': master_list[1][i]['recovered'],
            'deaths': master_list[2][i]['deaths'],
            'mortality_rate': (master_list[2][i]['deaths'])/(master_list[0][i]['confirmed']),
            'recovery_rate': (master_list[1][i]['recovered'])/(master_list[0][i]['confirmed']),
        })

    dict_to_save_in_mongo['data'] = final_list



test()