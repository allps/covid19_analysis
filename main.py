from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route, Mount
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


async def confirmed(request):

    final_list = []
    os.chdir(os.getcwd() + "/static/")
    dataset = os.listdir()
    print(dataset)

    # read the dataset

    covid_df = pd.read_csv(dataset[0])

    # convert into date time format

    covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
    covid_df.drop(["SNo"], 1, inplace=True)
    print(covid_df.head())


   # grouping different types of cases as per the date

    datewise_df = covid_df.groupby(["ObservationDate"]).agg({"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    print(datewise_df.head())

    # extract the information from the dataset
    print("Total number of countries with Disease Spread: ", len(covid_df["Country/Region"].unique()))
    print("Total number of Confirmed Cases", datewise_df["Confirmed"].iloc[-1])
    print("Total number of Recovered Cases", datewise_df["Recovered"].iloc[-1])
    print("Total number of Deaths Cases", datewise_df["Deaths"].iloc[-1])

    # plot curve of no of confirmed cases

    arr_yax = datewise_df['Confirmed'].to_numpy()
    print(arr_yax)
    y_list = arr_yax.tolist()
    final_list.append(y_list)

    arr_xax = datewise_df.ObservationDate.to_numpy()
    x_list = arr_xax.tolist()
    final_list.append(x_list)

    json_str = json.dumps(final_list)
    return JSONResponse(json_str)


async def mortalityRate(request):

    final_list = []
    os.chdir(os.getcwd() + "/static/")
    dataset = os.listdir()

    # read the dataset
    covid_df = pd.read_csv(dataset[0])

    # convert into date time format

    covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
    covid_df.drop(["SNo"], 1, inplace=True)

    datewise_df = covid_df.groupby(["ObservationDate"]).agg({"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    datewise_df["Mortality"] = (datewise_df["Deaths"] / datewise_df["Confirmed"]) * 100

    arr_yax = datewise_df['Mortality'].to_numpy()
    print(arr_yax)
    y_list = arr_yax.tolist()
    final_list.append(y_list)

    arr_xax = datewise_df.ObservationDate.to_numpy()
    x_list = arr_xax.tolist()
    final_list.append(x_list)

    json_str = json.dumps(final_list)
    return JSONResponse(json_str)

async def recoveryRate(request):

    final_list = []
    os.chdir(os.getcwd() + "/static/")
    dataset = os.listdir()
    print(dataset)

    # read the dataset

    covid_df = pd.read_csv(dataset[0])

    # convert into date time format

    covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
    covid_df.drop(["SNo"], 1, inplace=True)

    datewise_df = covid_df.groupby(["ObservationDate"]).agg({"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    datewise_df["Recovery"] = (datewise_df["Recovered"] / datewise_df["Confirmed"]) * 100

    arr_yax = datewise_df['Recovery'].to_numpy()
    print(arr_yax)
    y_list = arr_yax.tolist()
    final_list.append(y_list)

    arr_xax = datewise_df.ObservationDate.to_numpy()
    x_list = arr_xax.tolist()
    final_list.append(x_list)

    json_str = json.dumps(final_list)
    return JSONResponse(json_str)


async def countrywise(request):
    final_list = []
    req = request.path_params['country']
    print(req)
    os.chdir(os.getcwd() + "/static/")
    dataset = os.listdir()

    # read the dataset
    covid_df = pd.read_csv(dataset[0])
    # convert into date time format

    covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
    covid_df.drop(["SNo"], 1, inplace=True)
    country_data = covid_df[covid_df["Country/Region"] == req]
    datewise_country = country_data.groupby(["ObservationDate"]).agg({"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
    print(datewise_country.tail())

    arr_yax = datewise_country['Confirmed'].to_numpy()
    print(arr_yax)
    y_list = arr_yax.tolist()
    final_list.append(y_list)

    arr_xax = datewise_country.ObservationDate.to_numpy()
    x_list = arr_xax.tolist()
    final_list.append(x_list)

    #total of different cases per country
    total_cases = datewise_country.iloc[-1]
    print(total_cases)

    json_str = json.dumps(final_list)
    return JSONResponse(json_str)

routes = [
    Mount('/static', app=StaticFiles(directory='static'), name='static'),

    ################# daywise Analysis#################
    Route('/cases/confirmed', endpoint=confirmed, methods=["GET"]),
    Route('/mortalityRate', endpoint=mortalityRate, methods=["GET"]),
    Route('/recoveryRate', endpoint=recoveryRate, methods=["GET"]),

    ################country wise Analysis ##############
    Route('/country/{country}', endpoint=countrywise, methods=["GET", "POST"]),

]

app = Starlette(debug=True, routes=routes)
