import logging
import os
import requests
import shutil
import time
from starlette.responses import JSONResponse

from config import remote_urls, dataset_directory_path


def clear_all_temp_data(request):
    if os.path.isdir(dataset_directory_path):
        shutil.rmtree(dataset_directory_path)
        logging.info('Cleared')
        return JSONResponse({'message': dataset_directory_path + ' has been cleared.'}, status_code=200)
    return JSONResponse({'message': dataset_directory_path + ' does not exist.'}, status_code=200)


def refresh_data(request):
    this_moment_in_time = 'time_series' + os.sep + str(time.time()).split('.')[0]
    time_series_file_list = {
        'confirmed': fetch_file_from_url(remote_urls['confirmed_time_series'],
                                         'time_series_confirmed' + '.csv',
                                         directory_to_save=dataset_directory_path + this_moment_in_time + os.sep),
        'recovered': fetch_file_from_url(remote_urls['recovered_time_series'],
                                         'time_series_recovered' + '.csv',
                                         directory_to_save=dataset_directory_path + this_moment_in_time + os.sep),
        'deaths': fetch_file_from_url(remote_urls['deaths_time_series'],
                                      'time_series_deaths' + '.csv',
                                      directory_to_save=dataset_directory_path + this_moment_in_time + os.sep)
    }

    return JSONResponse(time_series_file_list, status_code=200)


def fetch_file_from_url(remote_url: str, filename: str, method: str = 'GET', directory_to_save: str = '') -> str:
    """
    Fetches a file from remote URL.
    Returns the absolute path of downloaded file OR throws an exception.
    :param directory_to_save:
    :param remote_url: str, url of the file to be fetched
    :param filename: str, file name of the remote file with which it is supposed to be saved
    :param method: str, HTTP request type. Defaults to GET
    :return: str, absolute path of the downloaded file on disk
    """
    if directory_to_save == '':
        directory_to_save = dataset_directory_path

    logging.info('Begin download of ' + remote_url)

    if not os.path.isdir(directory_to_save):
        os.makedirs(directory_to_save, exist_ok=True)

    # do not download in all at once. Stream instead.
    with requests.get(remote_url, stream=True) as resp:
        with open(directory_to_save + filename, 'wb') as f:
            # Do not overflow the RAM. Download the stream in chunks of 1MB
            for chunk in resp.iter_content(1000000):
                f.write(chunk)

    f.close()
    logging.info('Downloaded ' + remote_url + ' to ' + directory_to_save + filename)
    return directory_to_save + filename


def get_latest_time_series_file_dict() -> dict:
    file_list = []
    for item in os.listdir(dataset_directory_path + 'time_series'):
        file_list.append(item)

    return {
        'confirmed': dataset_directory_path + 'time_series' + os.sep + file_list[
            -1] + os.sep + 'time_series_confirmed.csv',
        'recovered': dataset_directory_path + 'time_series' + os.sep + file_list[
            -1] + os.sep + 'time_series_recovered.csv',
        'deaths': dataset_directory_path + 'time_series' + os.sep + file_list[-1] + os.sep + 'time_series_deaths.csv'
    }
