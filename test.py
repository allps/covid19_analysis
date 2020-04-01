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
        total_countries_affected = len(df["Country/Region"].unique())

        dict_to_save_in_mongo['totalCountries'] = total_countries_affected
        dict_to_save_in_mongo[key] = int((df.iloc[:, -1]).sum())


        print(total_countries_affected)
        print(key + ': ' + str(last_column.sum()))

    print(cases_list)
    print(dates_list)

test()