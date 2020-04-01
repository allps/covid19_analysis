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



test()