from starlette.responses import JSONResponse
from datetime import datetime
import pandas as pd
import requests, os, time, zipfile, shutil, logging, re
from config import remote_urls, dataset_directory_path

from pymongo import MongoClient


def clear_all_temp_data(request):
    if os.path.isdir(dataset_directory_path):
        shutil.rmtree(dataset_directory_path)
        return JSONResponse({'message': dataset_directory_path + ' has been cleared.'}, status_code=200)
    return JSONResponse({'message': dataset_directory_path + ' does not exist.'}, status_code=200)


def refresh_data(request):
    print(request.path_params['file_type'])

    if request.path_params['file_type'] == 'zip':
        filename = 'john_hopkins_repo_' + str(time.time()) + '.zip'
        dataset_zip_path = fetch_file_from_url(remote_urls['john_hopkins_repo'], filename)
        return JSONResponse({'message': extract_zipfile(dataset_zip_path)}, status_code=200)

    if request.path_params['file_type'] == 'ts':
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

    message = request.path_params['file_type'] + ' does not match any known operation types. Try \'zip\' or \'ts\''
    return JSONResponse({'message': message}, status_code=422)


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


def extract_zipfile(filepath: str, extract_directory: str = "") -> str:
    """
    Extracts a zip file to a directory
    :param filepath: str, absolute path os the zip file
    :param extract_directory: str, absolute path of the directory where it has to be extracted.
                                    Defaults to parent directory of the zip file
    :return: str, absolute path of the directory where zip is extracted
    """

    logging.info('Begin extracting ' + filepath)

    if extract_directory == '':
        extract_directory = os.path.split(filepath)[0] + os.sep + os.path.split(filepath)[1].replace('zip', '')
    else:
        os.makedirs(extract_directory, exist_ok=True)

    with zipfile.ZipFile(filepath, "r") as z:
        z.extractall(extract_directory)

    logging.info('Extracted ' + filepath + ' to ' + extract_directory)
    return extract_directory


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


def get_combined_time_series_data_set(dataset_directory: str):
    """
    Returns a row wise dictionary of all dataset files combined from
    time series data directory
    :param dataset_directory: str, directory of the time-series dataset
    :return: dict, a row wise dictionary created from the data frame
    """
    filename_regex = r'^((0|1)\d{1})-((0|1|2)\d{1})-((19|20)\d{2}).csv'
    dataset_file_list = []

    for item in os.listdir(dataset_directory):
        if re.match(filename_regex, item):
            dataset_file_list.append(item)

    dataset_file_list.sort(key=lambda date: datetime.strptime(date.replace('.csv', ''), '%m-%d-%Y'))

    mini_data_frame_list = []

    for item in dataset_file_list:
        mini_data_frame_list.append(pd.read_csv(dataset_directory + os.sep + item))

    final_data_frame = pd.concat(mini_data_frame_list)
    final_data_frame.reset_index(drop=True, inplace=True)
    final_data_frame.fillna('', inplace=True)

    final_data_frame["Confirmed"] = pd.to_numeric(final_data_frame['Confirmed'], errors='coerce')
    final_data_frame["Recovered"] = pd.to_numeric(final_data_frame['Recovered'], errors='coerce')
    final_data_frame["Deaths"] = pd.to_numeric(final_data_frame['Deaths'], errors='coerce')

    final_data_frame["Last Update"] = pd.to_datetime(final_data_frame["Last Update"])
    datewise_df = final_data_frame.groupby(["Last Update"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

    countrywise_df = final_data_frame.groupby(["Country/Region"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

    countrywise_df.drop(countrywise_df.index[0], inplace=True)

    countrywise_df2 = final_data_frame.groupby(["Country_Region"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

    countrywise_df2.drop(countrywise_df2.index[0], inplace=True)
    countrywise_df2.rename(columns={"Country_Region": "Country/Region"}, inplace=True)
    countrywise_df3 = pd.concat([countrywise_df, countrywise_df2]).drop_duplicates().reset_index(drop=True)
    countrywise_df3["perCountryMortality"] = (countrywise_df3["Deaths"] / countrywise_df3["Confirmed"]) * 100

    countrywise_df3["Country/Region"] = countrywise_df3["Country/Region"].str.replace(' ', '')

    countrywise_df3.drop_duplicates(subset="Country/Region", keep="last", inplace=True)
    countrywise_df3['Country/Region'] = countrywise_df3['Country/Region'].str.lower()

    arr_recovered = datewise_df['Recovered'].to_numpy()
    arr_deaths = datewise_df['Deaths'].to_numpy()
    arr_confirmed = datewise_df['Confirmed'].to_numpy()
    arr_xax = datewise_df['Last Update'].to_numpy()

    recovered_list = arr_recovered.tolist()
    death_list = arr_deaths.tolist()
    confirmed_list = arr_confirmed.tolist()
    x_list = arr_xax.tolist()
    datewise_df.fillna(0, inplace=True)
    datewise_df["Mortality"] = (datewise_df["Deaths"] / datewise_df["Confirmed"]) * 100
    datewise_df["Recovery"] = (datewise_df["Recovered"] / datewise_df["Confirmed"]) * 100

    datewise_df.fillna(0, inplace=True)

    arr_mortality = datewise_df['Mortality'].to_numpy()
    mortality_list = arr_mortality.tolist()

    arr_recovery = datewise_df['Recovery'].to_numpy()
    recovery_list = arr_recovery.tolist()

    total_confirmed_cases = datewise_df["Confirmed"].sum()
    total_recovered_cases = datewise_df["Recovered"].sum()
    total_deaths = datewise_df["Deaths"].sum()

    countrywise_plot_mortal = countrywise_df3[countrywise_df3["Confirmed"] > 50].sort_values(["perCountryMortality"],
                                                                                             ascending=False).head(225)

    arr_countries = countrywise_plot_mortal['Country/Region'].to_numpy()
    countries_list = arr_countries.tolist()

    arr_perCountry_mortality = countrywise_plot_mortal['perCountryMortality'].to_numpy()
    arr_perCountry_mortality_list = arr_perCountry_mortality.tolist()

    dictionary = {
        "json_xax": x_list,
        "confirmed": confirmed_list,
        "recovered": recovered_list,
        "death": death_list,
        "mortality": mortality_list,
        "recoveryRate": recovery_list,
        "totalNumberConfirmed": total_confirmed_cases,
        "totalRecovered": total_recovered_cases,
        "totalDeaths": total_deaths,
        "countries": countries_list,
        "perCountryMortality": arr_perCountry_mortality_list
    }

    # for country_table data---------
    country_records = countrywise_df3.to_dict(orient='records')

    mongo_client = MongoClient('mongodb://localhost:27017/')
    mydb = mongo_client["covid19"]
    mycol2 = mydb["countries_table"]
    mycol2.insert_many(country_records)

    # for countryWise data (saved basic data of each country)--------

    dateTimeObj = datetime.now()
    print(dateTimeObj)
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")

    print('Current Timestamp : ', timestampStr)

    country_wise_data_to_be_thrown_into_db = []
    for row_number in countrywise_df3.index:
        tmp_dict = {
            "name": countrywise_df3['Country/Region'][row_number],
            "basic": {
                "confirmed": countrywise_df3['Confirmed'][row_number],
                "recovered": countrywise_df3['Recovered'][row_number],
                "deaths": countrywise_df3['Deaths'][row_number],
                "mortality": countrywise_df3['perCountryMortality'][row_number]
            },
            "detailed": {},
            "created_at": timestampStr
        }
        country_wise_data_to_be_thrown_into_db.append(tmp_dict)

    mongo_client = MongoClient('mongodb://localhost:27017/')
    mydb = mongo_client["covid19"]
    mydb["country_wise_data"].insert_many(country_wise_data_to_be_thrown_into_db)

    return dictionary
