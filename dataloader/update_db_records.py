from starlette.responses import PlainTextResponse, JSONResponse
from .data_config import remote_urls, dataset_directory_path
from .load_data import fetch_file_from_url, extract_zipfile

import time, os, re


async def update_db(request):
    return JSONResponse({'message': 'Updated records in the database'}, status_code=200)


def update_cumulative_cases_record():
    # get the name of the latest zip file in data path
    dataset_file_list = []
    filename_regex = r'([a-zA-Z0-9\s_\\.\-\(\):])+(.zip)$'
    for item in os.listdir(dataset_directory_path):
        if re.match(filename_regex, item):
            dataset_file_list.append(item)

    t = dataset_directory_path + dataset_file_list[-1].replace('zip', '') + os.sep + remote_urls['daily_reports_relative_path']
    return JSONResponse({'files': t}, status_code=200)

    # filename = 'john_hopkins_repo_' + str(time.time()) + '.zip'
    # dataset_zip_path = fetch_file_from_url(remote_urls['john_hopkins_repo'], filename)
    # extracted_dir = extract_zipfile(dataset_zip_path)
