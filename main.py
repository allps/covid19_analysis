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

    arr_xax = datewise_df.ObservationDate.to_numpy()
    print(arr_xax)
    x_list = arr_xax.tolist()
    y_list.append(x_list)

    json_str = json.dumps(y_list)

    return JSONResponse(json_str)


async def mortalityRate(request):
    return JSONResponse("qwertyu")

routes = [
    Mount('/static', app=StaticFiles(directory='static'), name='static'),

    ################# daywise #################
    Route('/cases/confirmed', endpoint=confirmed, methods=["GET"]),
    Route('/mortalityRate', endpoint=mortalityRate, methods=["GET"]),

]

app = Starlette(debug=True, routes=routes)
