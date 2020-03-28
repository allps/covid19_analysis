import json
import os
import uvicorn

import pandas as pd
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route

from dataloader import refresh_data, clear_all_temp_data, update_db
from api import all_cases, total_cases_count


async def totalCases(request):
    try:
        dataset_directory_path = os.getcwd() + "/data/"

        dataset_file_list = os.listdir(dataset_directory_path)

        # read the dataset
        covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])

        covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
        covid_df.drop(["SNo"], 1, inplace=True)

        print(covid_df.isnull().sum())

        datewise_df = covid_df.groupby(["ObservationDate"]).agg(
            {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

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
        return JSONResponse(dictionary)
    except:
        return JSONResponse("Something Wrong")


async def confirmed(request):
    try:
        dataset_directory_path = os.getcwd() + "/data/"

        dataset_file_list = os.listdir(dataset_directory_path)

        # read the dataset
        covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])

        covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
        covid_df.drop(["SNo"], 1, inplace=True)

        # grouping different types of cases as per the date

        datewise_df = covid_df.groupby(["ObservationDate"]).agg(
            {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

        # plot curve of no of confirmed cases
        arr_yax = datewise_df['Confirmed'].to_numpy()
        print(arr_yax)
        y_list = arr_yax.tolist()

        arr_xax = datewise_df.ObservationDate.to_numpy()
        x_list = arr_xax.tolist()

        arr_recovered = datewise_df['Recovered'].to_numpy()
        arr_deaths = datewise_df['Deaths'].to_numpy()
        recovered_list = arr_recovered.tolist()
        death_list = arr_deaths.tolist()

        dictionary = {
            "json_xax": x_list,
            "confirmed": y_list,
            "recovered": recovered_list,
            "death": death_list
        }

        json_str = json.dumps(dictionary)
        return JSONResponse(dictionary)
    except:
        return JSONResponse("something wrong")


async def mortalityRate(request):
    try:
        dataset_directory_path = os.getcwd() + "/data/"

        dataset_file_list = os.listdir(dataset_directory_path)

        # read the dataset
        covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])

        # convert into date time format

        covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
        covid_df.drop(["SNo"], 1, inplace=True)

        datewise_df = covid_df.groupby(["ObservationDate"]).agg(
            {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
        datewise_df["Mortality"] = (datewise_df["Deaths"] / datewise_df["Confirmed"]) * 100

        arr_yax = datewise_df['Mortality'].to_numpy()
        print(arr_yax)
        y_list = arr_yax.tolist()

        arr_xax = datewise_df['ObservationDate'].to_numpy()
        x_list = arr_xax.tolist()

        dictionary = {
            "json_xax": x_list,
            "json_yax": y_list
        }

        json_str = json.dumps(dictionary)
        return JSONResponse(json_str)
    except:
        return JSONResponse("Something wrong")


async def recoveryRate(request):
    try:
        dataset_directory_path = os.getcwd() + "/data/"

        dataset_file_list = os.listdir(dataset_directory_path)

        # read the dataset

        covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])

        # convert into date time format

        covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
        covid_df.drop(["SNo"], 1, inplace=True)

        datewise_df = covid_df.groupby(["ObservationDate"]).agg(
            {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

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
    except:
        return JSONResponse("Something wrong")


async def countrywise(request):
    try:
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
        datewise_country = country_data.groupby(["ObservationDate"]).agg(
            {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

        arr_yax = datewise_country['Confirmed'].to_numpy()
        print(arr_yax)
        y_list = arr_yax.tolist()

        arr_xax = datewise_country.ObservationDate.to_numpy()
        x_list = arr_xax.tolist()

        arr_recovered = datewise_country['Recovered'].to_numpy()
        arr_deaths = datewise_country['Deaths'].to_numpy()
        recovered_list = arr_recovered.tolist()
        death_list = arr_deaths.tolist()

        dictionary = {
            "json_xax": x_list,
            "confirmed": y_list,
            "recovered": recovered_list,
            "death": death_list
        }

        json_str = json.dumps(dictionary)
        return JSONResponse(json_str)
    except:
        return JSONResponse("Something Wrong")


async def countryMortalityRate(request):
    try:
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
    except:
        return JSONResponse("Something Wrong")


async def countryRecoveryRate(request):
    try:
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

        datewise_country = country_data.groupby(["ObservationDate"]).agg(
            {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()

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
    except:
        return JSONResponse("Something Wrong")


async def countryTotalCases(request):
    try:
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
    except:
        return JSONResponse("Something Wrong")


async def perCountrymortality(request):
    try:
        dataset_directory_path = os.getcwd() + "/data/"

        dataset_file_list = os.listdir(dataset_directory_path)
        print(dataset_directory_path + dataset_file_list[0])

        # read the dataset
        covid_df = pd.read_csv(dataset_directory_path + dataset_file_list[0])
        # convert into date time format
        covid_df["ObservationDate"] = pd.to_datetime(covid_df["ObservationDate"])
        covid_df.drop(["SNo"], 1, inplace=True)
        countrywise_df = covid_df.groupby(["Country/Region"]).agg(
            {"Confirmed": 'sum', "Recovered": 'sum', "Deaths": 'sum'}).reset_index()
        print(countrywise_df.head())

        countrywise_df["Mortality"] = (countrywise_df["Deaths"] / countrywise_df["Confirmed"]) * 100

        countrywise_plot_mortal = countrywise_df[countrywise_df["Confirmed"] > 50].sort_values(["Mortality"],
                                                                                               ascending=False).head(25)
        arr_yax = countrywise_plot_mortal['Country/Region'].to_numpy()
        print(arr_yax)
        y_list = arr_yax.tolist()

        arr_xax = countrywise_plot_mortal['Mortality'].to_numpy()
        x_list = arr_xax.tolist()

        dictionary = {
            "json_xax": x_list,
            "json_yax": y_list
        }

        json_str = json.dumps(dictionary)
        return JSONResponse(dictionary)
    except:
        return JSONResponse("something wrong")


routes = [
    # Mount('/static', app=StaticFiles(directory='static'), name='static'),

    ################# daywise Analysis (worldwide) #################
    Route('/cases/total', endpoint=total_cases_count, methods=["GET"]),
    Route('/cases', endpoint=all_cases, methods=["GET"]),
    Route('/mortalityRate', endpoint=mortalityRate, methods=["GET"]),
    Route('/recoveryRate', endpoint=recoveryRate, methods=["GET"]),
    Route('/perCountry/mortality', endpoint=perCountrymortality, methods=['GET']),

    ################ country wise Analysis ##############

    Route('/country/{country}', endpoint=countrywise, methods=["GET", "POST"]),
    Route('/country/totalCases/{country}', endpoint=countryTotalCases, methods=["GET", "POST"]),
    Route('/country/mortalityRate/{country}', endpoint=countryMortalityRate, methods=["GET", "POST"]),
    Route('/country/recoveryRate/{country}', endpoint=countryRecoveryRate, methods=["GET", "POST"]),


    Route('/system/refresh_data', endpoint=refresh_data, methods=['GET']),
    Route('/system/clear_all', endpoint=clear_all_temp_data, methods=['GET']),
    Route('/system/refresh-database', endpoint=update_db, methods=['GET'])

]

middleware = [
    Middleware(CORSMiddleware, allow_origins=['*'])
]

app = Starlette(debug=True, routes=routes, middleware=middleware)

if __name__ == "__main__":
    uvicorn.run('main:app', host="0.0.0.0", port=8000, reload=True)
