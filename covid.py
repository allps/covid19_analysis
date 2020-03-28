import uvicorn
import pandas as pd
import json
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route
import pymongo
from pymongo import MongoClient
from dataloader import refresh_data

mongo_client = MongoClient('mongodb://localhost:27017/')


async def allCases(request):
    mydb = mongo_client["covid19"]
    mycol = mydb["visualizations"]

    for x in mycol.find({}, {"_id": 0, "json_xax": 1, "confirmed": 1, "recovered": 1, "death": 1}):
        print("qwertyu")

    return JSONResponse(x)


async def mortalityRate(request):
    mydb = mongo_client["covid19"]
    mycol = mydb["visualizations"]

    for x in mycol.find({}, {"_id": 0, "json_xax": 1, "mortality": 1}):
        print("qwertyu")

    return JSONResponse(x)


async def recoveryRate(request):
    mydb = mongo_client["covid19"]
    mycol = mydb["visualizations"]

    for x in mycol.find({}, {"_id": 0, "json_xax": 1, "recoveryRate": 1}):
        print("qwertyu")

    return JSONResponse(x)


async def totalCasesCount(request):
    mydb = mongo_client["covid19"]
    mycol = mydb["visualizations"]

    for x in mycol.find({}, {"_id": 0, "totalNumberConfirmed": 1, "totalRecovered": 1, "totalDeaths": 1}):
        print(x)

    return JSONResponse(x)

async def perCountryMortality(request):
    mydb = mongo_client["covid19"]
    mycol = mydb["visualizations"]

    for x in mycol.find({}, {"_id": 0, "countries": 1, "perCountryMortality": 1}):
        print("qwertyu")

    return JSONResponse(x)

async def show_countries_table(request):
    mydb = mongo_client["covid19"]
    # mycol2 = mydb["countries_table"]
    myresults = list(mydb["countries_table"].find({}, {"_id": 0}))
    # print(myresults)

    return JSONResponse(myresults)

routes = [
    # Mount('/static', app=StaticFiles(directory='static'), name='static'),

    ################# daywise Analysis (worldwide) #################

    Route('/v1/cases', endpoint=allCases, methods=["GET"]),
    Route('/v1/mortalityRate', endpoint=mortalityRate, methods=["GET"]),
    Route('/v1/recoveryRate', endpoint=recoveryRate, methods=["GET"]),
    Route('/v1/cases/total', endpoint=totalCasesCount, methods=["GET"]),
    Route('/v1/perCountry/mortality', endpoint=perCountryMortality, methods=['GET']),
    Route('/v1/countriesTable', endpoint=show_countries_table, methods=['GET']),

    Route('/system/refresh_data', endpoint=refresh_data, methods=['GET']),
    Route('/system/clear_all', endpoint=refresh_data, methods=['GET'])

]

middleware = [
    Middleware(CORSMiddleware, allow_origins=['*'])
]

app1 = Starlette(debug=True, routes=routes, middleware=middleware)

if __name__ == "__main__":
    uvicorn.run(app1, host="0.0.0.0", port=8000)
