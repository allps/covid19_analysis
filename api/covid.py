from starlette.responses import JSONResponse
from starlette.routing import Route
from pymongo import MongoClient
from dataloader import refresh_data
from config import mongo_db_url, database_name


async def total_cases_count(request):
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.visualizations
        x = collection.find_one({'viz_type': 'total_cases_global'}, {"_id": 0})
        return JSONResponse(x)


async def all_cases_cumulative_global(request):
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.visualizations
        x = collection.find_one({'viz_type': 'all_cases_cumulative_global'}, {"_id": 0})
        return JSONResponse(x)


async def country_wise_mortality(request):
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.visualizations
        x = collection.find_one({'viz_type': 'country_wise_mortality'}, {"_id": 0})
        return JSONResponse(x)




# async def mortalityRate(request):
#     mydb = mongo_client["covid19"]
#     mycol =
#
#     for x in mycol.find({}, {"_id": 0, "json_xax": 1, "mortality": 1}):
#         print("qwertyu")
#
#     return JSONResponse(x)
#
#
# async def recoveryRate(request):
#     mydb = mongo_client["covid19"]
#     mycol = mydb["visualizations"]
#
#     for x in mycol.find({}, {"_id": 0, "json_xax": 1, "recoveryRate": 1}):
#         print("qwertyu")
#
#     return JSONResponse(x)
#
#

#
# async def perCountryMortality(request):
#     mydb = mongo_client["covid19"]
#     mycol = mydb["visualizations"]
#
#     for x in mycol.find({}, {"_id": 0, "countries": 1, "perCountryMortality": 1}):
#         print("qwertyu")
#
#     return JSONResponse(x)
#
# async def show_countries_table(request):
#     mydb = mongo_client["covid19"]
#     # mycol2 = mydb["countries_table"]
#     myresults = list(mydb["countries_table"].find({}, {"_id": 0}))
#     # print(myresults)
#
#     return JSONResponse(myresults)
#
# routes = [
#     # Mount('/static', app=StaticFiles(directory='static'), name='static'),
#
#     ################# daywise Analysis (worldwide) #################
#
#     Route('/v1/cases', endpoint=allCases, methods=["GET"]),
#     Route('/v1/mortalityRate', endpoint=mortalityRate, methods=["GET"]),
#     Route('/v1/recoveryRate', endpoint=recoveryRate, methods=["GET"]),
#     Route('/v1/cases/total', endpoint=totalCasesCount, methods=["GET"]),
#     Route('/v1/perCountry/mortality', endpoint=perCountryMortality, methods=['GET']),
#     Route('/v1/countriesTable', endpoint=show_countries_table, methods=['GET']),
#
#     Route('/system/refresh_data', endpoint=refresh_data, methods=['GET']),
#     Route('/system/clear_all', endpoint=refresh_data, methods=['GET'])
#
# ]
