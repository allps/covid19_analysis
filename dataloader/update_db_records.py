import logging
import os
import re
from datetime import datetime

import pandas as pd
import numpy as np
from starlette.responses import JSONResponse

from .data_config import remote_urls, dataset_directory_path


async def update_db(request):
    return update_cumulative_cases_record()


def get_latest_extracted_dataset_directory():
    zip_file_list = []
    filename_regex = r'([a-zA-Z0-9\s_\\.\-\(\):])+(.zip)$'
    for item in os.listdir(dataset_directory_path):
        if re.match(filename_regex, item):
            zip_file_list.append(item)

    return dataset_directory_path + zip_file_list[-1].replace('zip', '') + os.sep + remote_urls[
        'daily_reports_relative_path']


def get_sorted_dataset_latest_file_list(extracted_dataset_directory) ->list:
    filename_regex = r'^((0|1)\d{1})-((0|1|2)\d{1})-((19|20)\d{2}).csv'
    dataset_file_list = []

    for item in os.listdir(extracted_dataset_directory):
        if re.match(filename_regex, item):
            dataset_file_list.append(item)

    dataset_file_list.sort(key=lambda date: datetime.strptime(date.replace('.csv', ''), '%m-%d-%Y'))
    return dataset_file_list


def update_cumulative_cases_record():
    extracted_dataset_directory = get_latest_extracted_dataset_directory()
    mini_data_frame_list = []

    logging.info('Reading data from the latest directory: ' + extracted_dataset_directory)
    for item in get_sorted_dataset_latest_file_list(extracted_dataset_directory):
        mini_data_frame_list.append(pd.read_csv(extracted_dataset_directory + os.sep + item))

    final_data_frame = pd.concat(mini_data_frame_list)
    final_data_frame.reset_index(drop=True, inplace=True)
    final_data_frame.fillna('', inplace=True)
    logging.info('Created combined dataframe')
    print(final_data_frame.info())
    print(final_data_frame.head())
    print(final_data_frame.transpose().head())
    print('######################')
    print(np.where(pd.isnull(final_data_frame)))

    return JSONResponse({'message': 'Read data from the latest directory: ' + extracted_dataset_directory}, status_code=200)
