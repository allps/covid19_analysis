from starlette.responses import PlainTextResponse
import requests, os, time, zipfile, shutil

remote_urls = {
    'john_hopkins_repo': 'https://github.com/CSSEGISandData/COVID-19/archive/master.zip'
}
dataset_directory_path = os.getcwd() + "/data/"


def clear_all_temp_data(request):
    shutil.rmtree(dataset_directory_path)
    return PlainTextResponse('all cleared')


def refresh_data(request):
    # get data from url
    filename = 'john_hopkins_repo_' + str(time.time()) + '.zip'
    dataset_zip_path = fetch_file_from_url(remote_urls['john_hopkins_repo'], filename)
    extracted_dir = extract_zipfile(dataset_zip_path)

    return PlainTextResponse(extracted_dir)


def fetch_file_from_url(remote_url: str, filename: str, method: str = 'GET') -> str:
    """
    Fetches a file from remote URL.
    Returns the absolute path of downloaded file OR throws an exception.
    :param remote_url: str, url of the file to be fetched
    :param filename: str, file name of the remote file with which it is supposed to be saved
    :param method: str, HTTP request type. Defaults to GET
    :return: str, absolute path of the downloaded file on disk
    """

    if not os.path.isdir(dataset_directory_path):
        os.makedirs(dataset_directory_path, exist_ok=True)

    req = requests.get(remote_url)
    with open(dataset_directory_path + filename, 'wb') as f:
        for chunk in req.iter_content(100000):
            f.write(chunk)

    f.close()
    return dataset_directory_path + filename


def extract_zipfile(filepath: str, extract_directory: str = "") -> str:
    """
    Extracts a zip file to a directory
    :param filepath: str, absolute path os the zip file
    :param extract_directory: str, absolute path of the directory where it has to be extracted.
                                    Defaults to parent directory of the zip file
    :return: str, absolute path of the directory where zip is extracted
    """

    if extract_directory == '':
        extract_directory = os.path.split(filepath)[0] + os.sep + os.path.split(filepath)[1].replace('zip', '')
    else:
        os.makedirs(extract_directory, exist_ok=True)

    with zipfile.ZipFile(filepath, "r") as z:
        z.extractall(extract_directory)

    return extract_directory
