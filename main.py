'''
This is the script to fetch data from looker dashboard and save it to local file
'''


import sys
import getpass


# Make Chnages Here
BASE_URL = "https://me.cloud.looker.com/api/4.0"
DASHBOARD_ID = 193
FILENAME = "file.csv"

# API Endpoints
LOGIN = "/login"
DASHBOARD = "/dashboards/{}"
RUN_QUERY_GET_DATA = "/queries/{}/run/json"


def main():

    # Import Requests Module
    try:
        import requests
    except ImportError as e:
        print("Unable to import module named requests. perhaps it is not instlled.")
        _print_exception(e)

    # Import Pandas Module
    try:
        import pandas as pd
    except ImportError as e:
        print("Unable to import module named pandas. perhaps it is not instlled.")
        _print_exception(e)

    # Get Looker Auth Header
    try:
        headers = _get_auth_header(BASE_URL, LOGIN)
    except Exception as e:
        print("Got Error While Authetication to Looker.")
        _print_exception(e)

    # Get Dashboard Elements
    try:
        dashboards_elements = _get_dahboard_data(
            BASE_URL, DASHBOARD, DASHBOARD_ID, header)
    except Exception as e:
        print("Got Error While Fetching Dashboard Elements.")
        _print_exception(e)

    # Extract Dashboard Elements
    try:
        query_ids, slug = _get_query_id_and_slug(dashboards_elements)
    except Exception as e:
        print("Got Error While Extracting Dashboard Elements.")
        _print_exception(e)

    # Fetch Data By Query ID In Json Format
    try:
        data = _run_query_by_query_id(BASE_URL, RUN_QUERY_GET_DATA, query_ids)
    except Exception as e:
        print("Got Error While Fetching Data from Looker Using Query ID.")
        _print_exception(e)

    # Load Extracted Data to Pandas Dataframe and Save to File.
    try:
        _create_pandas_df_and_save_to_file(FILENAME, data)
    except Exception as e:
        print("Got Error While Saving Data to FIle.")
        _print_exception(e)

    print("Process Completed.")


def _get_auth_header(BASE_URL: str, LOGIN: str) -> dict:

    client_id = getpass.getpass(prompt="Enter Client ID : ")
    client_secret = getpass.getpass(prompt="Enter Client Secret : ")

    PARAMS = {'client_id': client_id,
              'client_secret':  client_secret
              }
    response = session.post(url=BASE_URL + LOGIN, params=PARAMS)
    if response.status_code in (200, 201):
        data = r.json()
        token = data['access_token']
        headers = {f'Authorization': "Bearer " + token}
        return headers
    else:
        raise requests.RequestException(
            f"Recived Invalid Resonse From Server. Status Code - {response.status_code}")


def _get_dahboard_data(BASE_URL: str, DASHBOARD: str, DASHBOARD_ID: int, header: dict) -> dict:
    rresponse = session.get(
        url=BASE_URL + DASHBOARD.format(DASHBOARD_ID), headers=headers)
    if response.status_code in (200, 201):
        dashboards_elements = response.json()['dashboard_elements']
        return dashboards_elements
    else:
        raise requests.RequestException(
            f"Recived Invalid Resonse From Server. Status Code - {response.status_code}")


def _get_query_id_and_slug(dashboards_elements: dict) -> tuple[[list], [list]]:
    query_ids = []
    slug = []
    for elem in dashboards_elements:
        query_ids.append(elem['result_maker']['query_id'])
        slug.append(elem['result_maker']['query']['slug'])
    return query_ids, slug


def _run_query_by_query_id(BASE_URL: str, RUN_QUERY_GET_DATA: str, query_ids: list) -> dict:
    response = session.get(
        url=BASE_URL + RUN_QUERY_GET_DATA.format(query_ids[0]), headers=headers)
    if response.status_code in (200, 201):
        data = response.json()
        return data
    else:
        raise requests.RequestException(
            f"Recived Invalid Resonse From Server. Status Code - {response.status_code}")


def _create_pandas_df_and_save_to_file(filename: str, data: dict) -> None:
    df = pd.DataFrame(data)
    df.to_csv(filename, sep=",")


def _print_exception(e):
    print(f"Error - {e}")
    sys.exit("Exiting Programme ...")


if __name__ == "__main__":
    main()
