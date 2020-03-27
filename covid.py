import uvicorn
import pandas as pd
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

async def mortalityRate():
    mydb = mongo_client["covid19"]
    mycol = mydb["visualizations"]
    return JSONResponse("qwertyu")

routes = [
    # Mount('/static', app=StaticFiles(directory='static'), name='static'),

    ################# daywise Analysis (worldwide) #################

    Route('/v1/cases', endpoint=allCases, methods=["GET"]),
    Route('/v1/mortalityRate', endpoint=mortalityRate, methods=["GET"]),
    # Route('/recoveryRate', endpoint=recoveryRate, methods=["GET"]),
    # Route('/perCountry/mortality', endpoint=perCountrymortality, methods=['GET']),
    # Route('/cases/total', endpoint=totalCases, methods=["GET"]),

    ################ country wise Analysis ##############

    # Route('/country/{country}', endpoint=countrywise, methods=["GET", "POST"]),
    # Route('/country/totalCases/{country}', endpoint=countryTotalCases, methods=["GET", "POST"]),
    # Route('/country/mortalityRate/{country}', endpoint=countryMortalityRate, methods=["GET", "POST"]),
    # Route('/country/recoveryRate/{country}', endpoint=countryRecoveryRate, methods=["GET", "POST"]),


    Route('/system/refresh_data', endpoint=refresh_data, methods=['GET']),
    Route('/system/clear_all', endpoint=refresh_data, methods=['GET'])

]

middleware = [
    Middleware(CORSMiddleware, allow_origins=['*'])
]

app1 = Starlette(debug=True, routes=routes, middleware=middleware)

if __name__ == "__main__":
    uvicorn.run(app1, host="0.0.0.0", port=8000)
