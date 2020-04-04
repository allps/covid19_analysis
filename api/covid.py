from starlette.responses import JSONResponse
from pymongo import MongoClient
from config import mongo_db_url, database_name


async def fetch_india_data_linegraph(request):
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.india_data
        x = collection.find_one({'viz_type': 'all_data'}, {"_id": 0})
        return JSONResponse(x)


async def fetch_india_regional_data_for_table(request):
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.india_data
        x = collection.find_one({'viz_type': 'all_regional_current_data'}, {"_id": 0})
        return JSONResponse(x)


async def fetch_us_states_basic_data_table(request):
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.us_data
        x = collection.find_one({'viz_type': 'total_cases_in_states'}, {"_id": 0})
        return JSONResponse(x)


async def fetch_us_states_case_data_list_bargraph(request):
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.us_data
        x = collection.find_one({'viz_type': 'states_case_visualization'}, {"_id": 0})
        return JSONResponse(x)


async def fetch_us_data(request):
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.us_data
        x = collection.find_one({'viz_type': "us_data_daywise_visualization"}, {"_id": 0})
        return JSONResponse(x)


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
        x = collection.find_one({'viz_type': 'country_wise_mortality_and_recovery_rates'}, {"_id": 0})
        return JSONResponse(x)


async def country_data_found(request):
    req = request.path_params['country']
    try:
        print(req)
        with MongoClient(mongo_db_url) as client:
            db = client[database_name]
            collection = db.country_wise_data
            myquery = {"name": req}
            mydoc = collection.find(myquery, {"_id": 0})
            for x in mydoc:
                print(x)
            return JSONResponse(x)
    except:
        return JSONResponse({"country not found": req})


async def country_data_visualization(request):
    req = request.path_params['country']
    try:
        with MongoClient(mongo_db_url) as client:
            db = client[database_name]
            collection = db.visualizations
            country_doc = collection.find_one({"viz_type": "country_wise_dict", "data.name": req},
                                              {"_id": 0, "data.$": 1})
            return JSONResponse(country_doc)
    except:
        return JSONResponse({"country data not found": req}, status_code=404)


async def show_countries_table(request):
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.countries_table
        myresults = list(collection.find({}, {"_id": 0}))
        return JSONResponse(myresults)


async def global_map_data(request):
    with MongoClient(mongo_db_url) as client:
        db = client[database_name]
        collection = db.visualizations
        x = collection.find_one({'viz_type': 'map_global'}, {"_id": 0})
        return JSONResponse(x)
