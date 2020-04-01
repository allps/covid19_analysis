from dataloader import get_latest_time_series_file_dict
from datetime import datetime
import time, re
import pandas as pd


def test():

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
        dates_list = [datetime.strptime(re.sub('/20$', '/2020', date_string), "%m/%d/%Y") for date_string in d]

        df.drop(['Country/Region', 'Province/State', 'Lat', 'Long'], axis=1, inplace=True)
        cases_list[key] = df.sum(axis=0).tolist()

    dict_to_save_in_mongo = {
        'viz_type': 'all_cases_cumulative_global',
        'confirmed': cases_list['confirmed'],
        'recovered': cases_list['recovered'],
        'death': cases_list['death'],
        'created_at': datetime.timestamp(datetime.now())
    }

    print(cases_list)
    print(dates_list)

test()