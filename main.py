from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route, Mount
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
from starlette.requests import Request
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import datetime as dt
from datetime import timedelta
import pandas as pd
import numpy as np
import os
import json

async def totalCases(request):
    dataset_directory_path = os.getcwd() + "/data/"

    dataset_file_list = os.listdir(dataset_directory_path)

    # read the dataset
    covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])

    covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
    covid_df.drop(["SNo"], 1, inplace=True)
    print(covid_df.head())

    datewise_df = covid_df.groupby(["ObservationDate"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    print(datewise_df.head())

    # extract the information from the dataset
    total_countries_affected = len(covid_df["Country/Region"].unique())
    total_confirmed_cases = datewise_df["Confirmed"].iloc[-1]
    total_recovered_cases = datewise_df["Recovered"].iloc[-1]
    total_deaths = datewise_df["Deaths"].iloc[-1]

    dictionary = {
        "totalCountries": total_countries_affected,
        "confirmed": total_confirmed_cases,
        "recovered": total_recovered_cases,
        "deaths": total_deaths
    }

    json_str = json.dumps(dictionary)
    return JSONResponse(json_str)


async def confirmed(request):

    # os.chdir(os.getcwd() + "/data/")
    dataset_directory_path = os.getcwd() + "/data/"

    dataset_file_list = os.listdir(dataset_directory_path)

    # read the dataset
    covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])

    covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
    covid_df.drop(["SNo"], 1, inplace=True)
    print(covid_df.head())


   # grouping different types of cases as per the date

    datewise_df = covid_df.groupby(["ObservationDate"]).agg({"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    print(datewise_df.head())

    # plot curve of no of confirmed cases

    arr_yax = datewise_df['Confirmed'].to_numpy()
    print(arr_yax)
    y_list = arr_yax.tolist()

    arr_xax = datewise_df.ObservationDate.to_numpy()
    x_list = arr_xax.tolist()

    dictionary = {
        "json_xax": x_list,
        "json_yax": y_list
    }

    json_str = json.dumps(dictionary)
    return JSONResponse(json_str)


async def mortalityRate(request):

    # os.chdir(os.getcwd() + "/data/")
    dataset_directory_path = os.getcwd() + "/data/"

    dataset_file_list = os.listdir(dataset_directory_path)

    # read the dataset
    covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])

    # convert into date time format

    covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
    covid_df.drop(["SNo"], 1, inplace=True)

    datewise_df = covid_df.groupby(["ObservationDate"]).agg({"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    datewise_df["Mortality"] = (datewise_df["Deaths"] / datewise_df["Confirmed"]) * 100

    arr_yax = datewise_df['Mortality'].to_numpy()
    print(arr_yax)
    y_list = arr_yax.tolist()

    arr_xax = datewise_df.ObservationDate.to_numpy()
    x_list = arr_xax.tolist()

    dictionary = {
        "json_xax": x_list,
        "json_yax": y_list
    }

    json_str = json.dumps(dictionary)
    return JSONResponse(json_str)

async def recoveryRate(request):

    # os.chdir(os.getcwd() + "/data/")
    dataset_directory_path = os.getcwd() + "/data/"

    dataset_file_list = os.listdir(dataset_directory_path)

    # read the dataset

    covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])

    # convert into date time format

    covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
    covid_df.drop(["SNo"], 1, inplace=True)

    datewise_df = covid_df.groupby(["ObservationDate"]).agg({"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    datewise_df["Recovery"] = (datewise_df["Recovered"] / datewise_df["Confirmed"]) * 100

    arr_yax = datewise_df['Recovery'].to_numpy()
    print(arr_yax)
    y_list = arr_yax.tolist()

    arr_xax = datewise_df.ObservationDate.to_numpy()
    x_list = arr_xax.tolist()
    dictionary = {
        "json_xax": x_list,
        "json_yax": y_list
    }

    json_str = json.dumps(dictionary)
    return JSONResponse(json_str)


async def countrywise(request):

    req = request.path_params['country']
    print(req)
    # os.chdir(os.getcwd() + "/data/")
    dataset_directory_path = os.getcwd() + "/data/"

    dataset_file_list = os.listdir(dataset_directory_path)
    print(dataset_directory_path + dataset_file_list[0])

    # read the dataset
    covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])
    print(covid_df.head())
    # convert into date time format
    covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
    covid_df.drop(["SNo"], 1, inplace=True)
    country_data = covid_df[covid_df["Country/Region"] == req]
    datewise_country = country_data.groupby(["ObservationDate"]).agg({"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    print(datewise_country.tail())

    arr_yax = datewise_country['Confirmed'].to_numpy()
    print(arr_yax)
    y_list = arr_yax.tolist()

    arr_xax = datewise_country.ObservationDate.to_numpy()
    x_list = arr_xax.tolist()
    dictionary = {
        "json_xax": x_list,
        "json_yax": y_list
    }

    json_str = json.dumps(dictionary)
    return JSONResponse(json_str)

async def countryMortalityRate(request):

    req = request.path_params['country']
    print(req)
    # os.chdir(os.getcwd() + "/data/")
    dataset_directory_path = os.getcwd() + "/data/"

    dataset_file_list = os.listdir(dataset_directory_path)
    print(dataset_directory_path + dataset_file_list[0])

    # read the dataset
    covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])
    # convert into date time format
    covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
    covid_df.drop(["SNo"], 1, inplace=True)
    country_data = covid_df[covid_df["Country/Region"] == req]
    datewise_country = country_data.groupby(["ObservationDate"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

    datewise_country["Mortality"] = (datewise_country["Deaths"] / datewise_country["Confirmed"]) * 100

    arr_yax = datewise_country['Mortality'].to_numpy()
    print(arr_yax)
    y_list = arr_yax.tolist()

    arr_xax = datewise_country.ObservationDate.to_numpy()
    x_list = arr_xax.tolist()

    dictionary = {
        "json_xax": x_list,
        "json_yax": y_list
    }

    json_str = json.dumps(dictionary)
    return JSONResponse(json_str)


async def countryRecoveryRate(request):
    req = request.path_params['country']
    print(req)
    # os.chdir(os.getcwd() + "/data/")
    dataset_directory_path = os.getcwd() + "/data/"

    dataset_file_list = os.listdir(dataset_directory_path)
    print(dataset_directory_path + dataset_file_list[0])

    # read the dataset
    covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])
    # convert into date time format
    covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
    covid_df.drop(["SNo"], 1, inplace=True)
    country_data = covid_df[covid_df["Country/Region"] == req]
    datewise_country = country_data.groupby(["ObservationDate"]).agg({"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

    datewise_country["Recovery"] = (datewise_country["Recovered"] / datewise_country["Confirmed"]) * 100
    arr_yax = datewise_country['Recovery'].to_numpy()
    print(arr_yax)
    y_list = arr_yax.tolist()

    arr_xax = datewise_country.ObservationDate.to_numpy()
    x_list = arr_xax.tolist()

    dictionary = {
        "json_xax": x_list,
        "json_yax": y_list
    }

    json_str = json.dumps(dictionary)
    return JSONResponse(json_str)

async def countryTotalCases(request):
    req = request.path_params['country']
    print(req)
    # os.chdir(os.getcwd() + "/data/")
    dataset_directory_path = os.getcwd() + "/data/"

    dataset_file_list = os.listdir(dataset_directory_path)
    print(dataset_directory_path + dataset_file_list[0])

    # read the dataset
    covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])
    # convert into date time format
    covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
    covid_df.drop(["SNo"], 1, inplace=True)
    country_data = covid_df[covid_df["Country/Region"] == req]
    datewise_country = country_data.groupby(["ObservationDate"]).agg(
        {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

    # total of different cases per country
    total_confirmed_cases = datewise_country["Confirmed"].iloc[-1]
    total_recovered_cases = datewise_country["Recovered"].iloc[-1]
    total_death_cases = datewise_country["Deaths"].iloc[-1]


    dictionary = {
        "confirmed": total_confirmed_cases,
        "recovered": total_recovered_cases,
        "deaths": total_death_cases
    }

    json_str = json.dumps(dictionary)

    return JSONResponse(json_str)


routes = [
    # Mount('/static', app=StaticFiles(directory='static'), name='static'),

    ################# daywise Analysis (worldwide) #################
    Route('/cases/total', endpoint=totalCases, methods=["GET"]),
    Route('/cases/confirmed', endpoint=confirmed, methods=["GET"]),
    Route('/mortalityRate', endpoint=mortalityRate, methods=["GET"]),
    Route('/recoveryRate', endpoint=recoveryRate, methods=["GET"]),

    ################ country wise Analysis ##############

    Route('/country/{country}', endpoint=countrywise, methods=["GET", "POST"]),
    Route('/country/totalCases/{country}', endpoint=countryTotalCases, methods=["GET"]),
    Route('/country/mortalityRate/{country}', endpoint=countryMortalityRate, methods=["GET", "POST"]),
    Route('/country/recoveryRate/{country}', endpoint=countryRecoveryRate, methods=["GET", "POST"]),

]

middleware = [
    Middleware(CORSMiddleware, allow_origins=['*'])
]

app = Starlette(debug=True, routes=routes, middleware=middleware)
