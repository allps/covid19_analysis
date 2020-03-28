import os

remote_urls = {
    'john_hopkins_repo': 'https://github.com/CSSEGISandData/COVID-19/archive/master.zip',
    'daily_reports_relative_path': 'COVID-19-master' + os.sep + 'csse_covid_19_data' + os.sep + 'csse_covid_19_daily_reports'
}
dataset_directory_path = os.getcwd() + os.sep + "data" + os.sep
