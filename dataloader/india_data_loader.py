from .update_db_records import update_records_in_database
from starlette.responses import JSONResponse
import requests


async def update_all_india_data(request):
    return JSONResponse({
        'historic_data': india_data_update_db(),
        'regional_data': update_regional_data()
    }, status_code=200)


def india_data_update_db():
    api_response = requests.get('https://api.rootnet.in/covid19-in/stats/history')
    dict_to_throw_in_json = api_response.json()
    dict_to_throw_in_json['viz_type'] = 'all_data'
    update_records_in_database('india_data', dict_to_throw_in_json, viz_type='all_data')
    return 'Updated historic data'


def update_regional_data():
    api_response = requests.get('https://api.rootnet.in/covid19-in/stats/latest')
    dict_to_throw_in_json = api_response.json()
    dict_to_throw_in_json['viz_type'] = 'all_regional_current_data'
    update_records_in_database('india_data', dict_to_throw_in_json, viz_type='all_regional_current_data')
    return 'Updated regional data'
