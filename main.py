import uvicorn

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import Route

from dataloader import refresh_data, clear_all_temp_data, update_db, \
    update_us_db, total_cases_statewise, state_visualization_bargraph, india_data_update_db, \
    india_current_regional_data_update_db, refresh_all

from api import country_data_visualization, show_countries_table

from api import all_cases_cumulative_global, total_cases_count, country_wise_mortality, \
    country_data_found, global_map_data, fetch_us_data, fetch_us_states_case_data_list_bargraph, \
    fetch_us_states_basic_data_table, fetch_india_data_linegraph, \
    fetch_india_regional_data_for_table

day_wise_analysis_worldwide_routes = [
    Route('/cases/total', endpoint=total_cases_count, methods=["GET"]),
    Route('/cases', endpoint=all_cases_cumulative_global, methods=["GET"]),
    Route('/perCountry/mortality', endpoint=country_wise_mortality, methods=['GET']),
]

country_wise_analysis_routes = [
    Route('/country/{country}', endpoint=country_data_found, methods=["GET", "POST"]),
    Route('/country/day-wise/{country}', endpoint=country_data_visualization, methods=["GET", "POST"]),
    Route('/countries-table', endpoint=show_countries_table, methods=["GET"]),
]

us_data_routes = [
    Route('/save/us-data', endpoint=update_us_db, methods=['GET']),
    Route('/save/total-cases-in-states', endpoint=total_cases_statewise, methods=['GET']),
    Route('/save/us-states/case-visualization', endpoint=state_visualization_bargraph, methods=['GET']),

    Route('/us-data/day-wise', endpoint=fetch_us_data, methods=['GET']),
    Route('/us-data/each-state', endpoint=fetch_us_states_case_data_list_bargraph, methods=['GET']),
    Route('/us-data/for-table', endpoint=fetch_us_states_basic_data_table, methods=['GET']),
]

india_data_routes = [
    Route('/india-data', endpoint=india_data_update_db, methods=['GET']),
    Route('/india-regional-data', endpoint=india_current_regional_data_update_db, methods=['GET']),

    Route('/india-data/day-wise', endpoint=fetch_india_data_linegraph, methods=['GET']),
    Route('/india-data/for-table', endpoint=fetch_india_regional_data_for_table, methods=['GET']),
]

system_routes = [
    Route('/system/refresh-data', endpoint=refresh_data, methods=['GET']),
    Route('/system/clear_all', endpoint=clear_all_temp_data, methods=['GET']),
    Route('/system/refresh-database', endpoint=update_db, methods=['GET']),
    Route('/system/refresh-all', endpoint=refresh_all, methods=['GET']),
]

routes = day_wise_analysis_worldwide_routes + country_wise_analysis_routes + us_data_routes + india_data_routes + \
         system_routes

routes.append(Route('/map/global', endpoint=global_map_data, methods=['GET']))

middleware = [
    Middleware(CORSMiddleware, allow_origins=['*'])
]

app = Starlette(debug=True, routes=routes, middleware=middleware)

if __name__ == "__main__":
    uvicorn.run('main:app', host="0.0.0.0", port=8000, reload=True)
