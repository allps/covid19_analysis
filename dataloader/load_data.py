from starlette.responses import PlainTextResponse, JSONResponse
from datetime import datetime
import pandas as pd
import pymongo
import requests, os, time, zipfile, shutil, logging, re, json

from pymongo import MongoClient
print("pymongo version:", pymongo.version)



remote_urls = {
    'john_hopkins_repo': 'https://github.com/CSSEGISandData/COVID-19/archive/master.zip'
}
dataset_directory_path = os.getcwd() + os.sep + "data" + os.sep


def clear_all_temp_data(request):
    shutil.rmtree(dataset_directory_path)
    return PlainTextResponse('all cleared')


def refresh_data(request):
    mongo_client = MongoClient('mongodb://localhost:27017/')

    mydb = mongo_client["covid19"]
    mycol = mydb["visualizations"]

    print(mongo_client.list_database_names())
    print(mydb.list_collection_names())

    # get data from url
    # filename = 'john_hopkins_repo_' + str(time.time()) + '.zip'
    # dataset_zip_path = fetch_file_from_url(remote_urls['john_hopkins_repo'], filename)
    # extracted_dir = extract_zipfile(dataset_zip_path)
    t = get_combined_time_series_data_set(
        r"C:\Users\91958\covid19_analysis\data\john_hopkins_repo_1585245624.2873893\COVID-19-master\csse_covid_19_data\csse_covid_19_daily_reports")

    mycol.insert(t)
    # print(x.inserted_ids)
    return JSONResponse("qwertyu")


def fetch_file_from_url(remote_url: str, filename: str, method: str = 'GET') -> str:
    """
    Fetches a file from remote URL.
    Returns the absolute path of downloaded file OR throws an exception.
    :param remote_url: str, url of the file to be fetched
    :param filename: str, file name of the remote file with which it is supposed to be saved
    :param method: str, HTTP request type. Defaults to GET
    :return: str, absolute path of the downloaded file on disk
    """

    logging.info('Begin download of ' + remote_url)

    if not os.path.isdir(dataset_directory_path):
        os.makedirs(dataset_directory_path, exist_ok=True)

    # do not download in all at once. Stream instead.
    with requests.get(remote_url, stream=True) as resp:
        with open(dataset_directory_path + filename, 'wb') as f:
            # Do not overflow the RAM. Download the stream in chunks of 1MB
            for chunk in resp.iter_content(1000000):
                f.write(chunk)

    f.close()
    logging.info('Downloaded ' + remote_url + ' to ' + dataset_directory_path + filename)
    return dataset_directory_path + filename


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
    print(final_data_frame.info())
    print('#################################################################')
    print(final_data_frame.head())
    print('#################################################################')
    print(final_data_frame.tail())
    final_data_frame.fillna('', inplace=True)
    print(final_data_frame.info())

    final_data_frame["Confirmed"] = pd.to_numeric(final_data_frame['Confirmed'], errors='coerce')
    final_data_frame["Recovered"] = pd.to_numeric(final_data_frame['Recovered'], errors='coerce')
    final_data_frame["Deaths"] = pd.to_numeric(final_data_frame['Deaths'], errors='coerce')

    print(final_data_frame.info())

    final_data_frame["Last Update"] = pd.to_datetime(final_data_frame["Last Update"])
    datewise_df = final_data_frame.groupby(["Last Update"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    print(datewise_df.head())

    countrywise_df = final_data_frame.groupby(["Country/Region"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

    print(countrywise_df.info())

    countrywise_df.drop(countrywise_df.index[0], inplace=True)
    print(countrywise_df.head())

    arr_recovered = datewise_df['Recovered'].to_numpy()
    arr_deaths = datewise_df['Deaths'].to_numpy()
    arr_confirmed = datewise_df['Confirmed'].to_numpy()
    arr_xax = datewise_df['Last Update'].to_numpy()

    recovered_list = arr_recovered.tolist()
    death_list = arr_deaths.tolist()
    confirmed_list = arr_confirmed.tolist()
    x_list = arr_xax.tolist()
    datewise_df.fillna(0, inplace=True)
    print('###############################################3')
    print(datewise_df.isnull().values.any())
    print('###############################################3')
    datewise_df["Mortality"] = (datewise_df["Deaths"] / datewise_df["Confirmed"]) * 100
    datewise_df["Recovery"] = (datewise_df["Recovered"] / datewise_df["Confirmed"]) * 100

    datewise_df.fillna(0, inplace=True)
    print('###############################################3')
    print(datewise_df.isnull().values.any())
    print('###############################################4')

    arr_mortality = datewise_df['Mortality'].to_numpy()
    mortality_list = arr_mortality.tolist()

    arr_recovery = datewise_df['Recovery'].to_numpy()
    recovery_list = arr_recovery.tolist()

    total_confirmed_cases = datewise_df["Confirmed"].sum()
    total_recovered_cases = datewise_df["Recovered"].sum()
    total_deaths = datewise_df["Deaths"].sum()

    countrywise_df["perCountryMortality"] = (countrywise_df["Deaths"] / countrywise_df["Confirmed"]) * 100
    countrywise_plot_mortal = countrywise_df[countrywise_df["Confirmed"] > 50].sort_values(["perCountryMortality"],
                                                                                           ascending=False).head(25)
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

    # t = dictionary.to_dict(orient='records')
    return dictionary

