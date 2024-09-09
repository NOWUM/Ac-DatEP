import os
from json import loads
import logging
from logging import getLogger, basicConfig, WARNING, INFO
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from io import StringIO
from typing import List
import time

import pandas as pd
import requests
from dotenv import load_dotenv
from geoalchemy2 import Geometry
from geopandas import GeoSeries
from pandas import DataFrame, read_csv, isna, concat, merge
from pandas.errors import ParserError
from shapely.geometry import Point
from sqlalchemy.dialects import postgresql
from requests import Session, HTTPError, RequestException
from requests.adapters import HTTPAdapter
from urllib3.exceptions import MaxRetryError, RequestError


logging_level_str = os.getenv("LOGGING_LEVEL", "ERROR")

postgresql.base.ischema_names['geometry'] = Geometry

class ExternalApiService:
    """
    ExternalApiService is a class designed to handle HTTP requests to external APIs efficiently and robustly.

    This service uses a session with a mounted HTTP adapter to manage connections and retries. It is optimized
    for making GET requests to external APIs, handling common HTTP errors, and logging them appropriately.

    Attributes:
        session (Session): A requests.Session object configured with a custom HTTPAdapter for connection pooling
                            and retry logic.
    """
    def __init__(self):
        """
        Initializes the ExternalApiService instance by setting up a requests.Session with a mounted HTTPAdapter.
        The adapter is configured for HTTPS connections with a specified number of pool connections and retries.
        """
        self.session = Session()
        self.session.mount('https://', HTTPAdapter(pool_connections=20, max_retries=3))

    def request_data(self, url: str, encoding: str = 'UTF-8'):
        """
        Makes a GET request to the specified URL and returns the response content.

        @param url: The URL to which the GET request is to be sent.
        @param encoding: The character encoding for decoding the response content. Defaults to 'UTF-8'.
        @return: The decoded content of the response, if the request is successful. Otherwise, returns None.

        Handles various HTTP-related errors, including connection errors, timeouts, and retry failures,
        logging them as errors. It also handles unexpected request exceptions.
        """
        with self.session as session:
            try:
                response = session.get(url, timeout=3)
                response.raise_for_status()
                return response.content.decode(encoding)
            except (HTTPError, ConnectionError, TimeoutError, MaxRetryError) as e:
                log.error(f'Error accessing {url}: {e}')
                return None
            except (RequestException, RequestError) as e:
                log.error(f'Unexpected error when accessing {url}: {e}')
                return None

log = getLogger("sensor_community")

datastream_dict = {
    'P1': {
        'type': 'PM10',
        'unit': 'µg/m³'
    },
    'P2': {
        'type': 'PM2.5',
        'unit': 'µg/m³'
    },
    'pressure': {
        'type': 'air pressure',
        'unit': 'Pa'
    },
    'temperature': {
        'type': 'temperature',
        'unit': '°C'
    },
    'humidity': {
        'type': 'humidity',
        'unit': '%'
    },
}


datastream_name_dict = {
    'P1': 'PM10',
    'P2': 'PM2.5',
    'pressure': 'air pressure',
    'temperature': 'temperature',
    'humidity': 'humidity'}


def fetch_community_data(url: str):
    """
    Fetches data from a given URL and loads it into a DataFrame.

    @param url: The URL from which to fetch data.
    @return: A DataFrame containing the loaded data.
    """
    client = ExternalApiService()

    try:
        if url:
            response = client.request_data(url)
            if url.endswith('.csv'):
                return load_csv_to_df(response)
            else:
                return load_json_to_df(response)
    except Exception as e:
        log.error(e)

def load_json_to_df(current_data: str):
    """
    Converts JSON string to a DataFrame.

    @param current_data: JSON string to be converted.
    @return: DataFrame created from the JSON data.
    """
    rows = []
    data = loads(current_data)
    for entry in data:
        row = {
            'timestamp': entry['timestamp'],
            'sensor_id': entry['sensor']['id'],
            'sensor_type': entry['sensor']['sensor_type']['name'],
            'manufacturer': entry['sensor']['sensor_type']['manufacturer'],
            'country': entry['location']['country'],
            'latitude': entry['location']['latitude'],
            'longitude': entry['location']['longitude'],
            'altitude': entry['location']['altitude']
        }
        for value in entry['sensordatavalues']:
            row[value['value_type']] = value['value']
        rows.append(row)

    return DataFrame(rows)


def load_csv_to_df(response_data: str):
    """
    Converts CSV string to a DataFrame.

    @param response_data: CSV string to be converted.
    @return: DataFrame created from the CSV data.
    """
    df = DataFrame()
    try:
        if response_data:
            df = read_csv(StringIO(response_data), delimiter=';', header=0, encoding='utf-8')
    except ParserError:
        log.error('An error occurred while reading and converting csv data.')
    return df


def create_sensors_df(initial_data: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of sensor information from the initial data.

    @param initial_data: The DataFrame containing the initial sensor data.
    @return: A DataFrame with processed sensor information.
    """
    sensor_data = initial_data[['sensor_id', 'sensor_type', 'manufacturer', 'longitude', 'latitude']]

    sensors = sensor_data.copy()
    sensors['source'] = 'SensorCommunity'
    sensors.rename(columns={'sensor_id': 'ex_id'}, inplace=True)
    sensors["ex_id"] = sensors["ex_id"].astype(str)
    sensors['description'] = sensors['manufacturer'] + ' - ' + sensors['sensor_type']
    sensors.drop(columns=['manufacturer', 'sensor_type'], axis=1, inplace=True)

    sensor_points = sensors.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
    geometry = GeoSeries(sensor_points)
    geometry.set_crs(epsg=25832, inplace=True)
    sensors['geometry'] = geometry.apply(lambda x: x.wkt)
    sensors['confidential'] = False

    return sensors


def create_datastreams_df(initial_data: DataFrame, sensors: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of datastreams based on sensor data.

    @param initial_data: The DataFrame containing the initial sensor data.
    @param sensors: The DataFrame containing processed sensor information.
    @return: A DataFrame with datastream information.
    """
    data: DataFrame = initial_data.loc[:, ['sensor_id'] + list(datastream_dict.keys())]

    sensors["ex_id"] = sensors["ex_id"].astype(str)
    data["sensor_id"] = data["sensor_id"].astype(str)
    data = data.merge(sensors, left_on='sensor_id', right_on='ex_id', how='left')
    data['sensor_id'] = data['ex_id'].fillna(data['sensor_id'])
    data.drop(columns=['sensor_id', 'ex_id'], inplace=True)

    datastreams = DataFrame(columns=['sensor_id', 'ex_id', 'type', 'unit', 'confidential'])
    for index, row in data.iterrows():
        sensor_id = row['id']

        for col in data.columns:
            if col in datastream_dict.keys() and not isna(row[col]):
                type_info = datastream_dict[col]['type']
                unit_info = datastream_dict[col]['unit']

                datastream = {'sensor_id': sensor_id, 'ex_id': -1, 'type': type_info, 'unit': unit_info,
                              'confidential': False}
                datastreams.loc[len(datastreams)] = datastream

    datastreams["ex_id"] = datastreams["ex_id"].astype(str)
    return datastreams


def create_measurements_df(response_data: DataFrame, datastreams: DataFrame,
                           sensors: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of measurements from response data.

    @param response_data: The DataFrame containing the response data.
    @param datastreams: The DataFrame containing datastream information.
    @param sensors: The DataFrame containing sensor information.
    @return: A DataFrame with measurement data.
    """

    try:

        # convert response data ID colum to str
        response_data["sensor_id"] = response_data["sensor_id"].astype(str)

        # rename the column to sensor_ex_id
        response_data.rename(
            columns={"sensor_id": "sensor_ex_id"},
            inplace=True)

        # convert from wide to long format
        response_data = response_data.melt(
            id_vars=["sensor_ex_id", "timestamp"],
            var_name="type")

        # remove rows with NaN values
        response_data.dropna(subset="value", inplace=True)

        # map type names from database onto dataframe
        response_data["type"] = response_data["type"].map(datastream_name_dict)

        # now we need to find the datastream ID for this
        # sensor ID and datastream type
        # 1st we combine sensors and datastreams into one DF
        sensors_datastreams = pd.merge(
            left=datastreams,
            right=sensors,
            left_on="sensor_id",
            right_on="id")

        # rename columns appropiately
        sensors_datastreams.rename(
            columns={
                "ex_id_x": "datastream_ex_id",
                "id_x": "datastream_id",
                "ex_id_y": "sensor_ex_id"},
            inplace=True)

        # we get the datastream IDs by merging
        # our new sensors_datastreams table containing the
        # additional info onto the response data by
        # on the "sensor_ex_id" colum and datastream "type"
        # column
        measurements = pd.merge(
            left=response_data,
            right=sensors_datastreams,
            on=["sensor_ex_id", "type"],
            how="left")

        # only use measurement table columns
        measurements = measurements[
            ["timestamp", "value", "datastream_id"]]

        # create column for confidentiality
        measurements["confidential"] = False

        # finally reset index
        measurements.reset_index(drop=True)

        return measurements

    except Exception as e:
        log.error(e)


def get_existing_data(endpoint: str, api_data: dict) -> DataFrame:
    """
    Retrieves data from a specified endpoint in the database via an API and returns it as a DataFrame.

    @param endpoint: The API endpoint to retrieve data from. Leading or trailing slashes will be removed.
    @param api_data: A dictionary containing the URL and authentication headers for the API.
                      Expected keys are 'url' for the API base URL and 'auth' for the authentication headers.

    @return: DataFrame: A pandas DataFrame containing the data retrieved from the API endpoint.

    Raises:
    - ValueError: If the response from the API is not successful.
    """
    endpoint = endpoint.strip("/\\")
    url = f"{api_data.get('url')}/{endpoint}"
    response = requests.get(url, headers=api_data.get('auth'))
    if response.status_code != 200:
        raise ValueError(f"Error retrieving data from endpoint {url}")
    return DataFrame(response.json())


def identify_new_data(new_data: DataFrame, existing_data: DataFrame,
                      compare_columns: str | List[str]) -> DataFrame:
    """
    Identifies new data entries in the initial_data DataFrame that do not match entries in the existing_sensors
    DataFrame based on specified columns.

    @param new_data: DataFrame containing the initial data.
    @param existing_data: DataFrame containing data from an existing dataset for comparison.
    @param compare_columns: Column name(s) in Dataframes to check for new entries. Can be a string for a single
            column or a list of strings for multiple columns.
    @return: DataFrame containing only the entries from initial_data that are not found in existing_sensors based on
            the specified columns.
    """
    if isinstance(compare_columns, str):
        compare_columns = [compare_columns]

    if new_data.empty:
        return existing_data
    elif existing_data.empty:
        return new_data
    else:
        mask = ~new_data.set_index(compare_columns).index.isin(existing_data.set_index(compare_columns).index)
        return new_data[mask]


def create_new_data(endpoint: str, new_data: DataFrame, api_data: dict, ) -> DataFrame:
    """
    Creates new sensors in the database via API.

    @param api_data: The URL and authentication information for database operations via API.
    @param new_data: The DataFrame containing new sensors to be created.
    @return: A DataFrame of created sensors.
    """

    try:
        if not new_data.empty:
            new_data.dropna(inplace=True)

            endpoint = endpoint.strip("/\\")
            url = f"{api_data.get('url')}/{endpoint}?on_duplicate=ignore"
            response = requests.post(url, json=new_data.to_dict(orient="records"), headers=api_data.get('auth'))
            if response.status_code != 200:
                raise ValueError(f"Error creating data at endpoint: {response.text}")
            if response.json() == {}:
                return DataFrame()
            else:
                return DataFrame(response.json())
        return DataFrame()

    except Exception as e:
        log.error(e)

def initialize_sensors_datastreams(api_data: dict, initial_data: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Initializes sensors and datastreams in the database and returns their DataFrames.

    @param api_data: The URL and authentication information for database operations via API.
    @param initial_data: The DataFrame containing the initial sensor data.
    @return: A tuple of DataFrames for sensors and datastreams.
    """

    try:
        initial_data.drop_duplicates(subset='sensor_id', inplace=True)

        existing_sensors = get_existing_data('sensors', api_data)
        existing_datastreams = get_existing_data('datastreams', api_data)

        initial_sensors = create_sensors_df(initial_data)
        new_sensors = identify_new_data(initial_sensors, existing_sensors, 'ex_id')
        existing_sensors = pd.concat([existing_sensors, create_new_data('sensors', new_sensors, api_data)])

        new_datastreams = create_datastreams_df(initial_data, existing_sensors)
        new_datastreams = identify_new_data(new_datastreams, existing_datastreams, ['sensor_id', 'type'])
        existing_datastreams = pd.concat([existing_datastreams, create_new_data('datastreams', new_datastreams, api_data)])

        return existing_sensors, existing_datastreams

    except Exception as e:
        log.error(e)


def get_archive_data(start_date: datetime.date, end_date: datetime.date, sensor_data: DataFrame, archive_url: str):
    """
    Retrieves archived data for a given date range and sensor data.

    @param start_date: The start date for the data retrieval.
    @param end_date: The end date for the data retrieval.
    @param sensor_data: The DataFrame containing sensor data.
    @param archive_url: The base URL for the archive data.
    @return: A DataFrame containing the archived data.
    """

    try:
        links = []
        for i in range((end_date - start_date).days + 1):
            request_date = start_date + timedelta(days=i)
            request_date_str = request_date.strftime('%Y-%m-%d')

            archive_urls = sensor_data.apply(
                lambda row:
                f"{archive_url}{request_date_str}/{request_date_str}_{str(row['sensor_type']).lower()}_sensor_{str(row['sensor_id'])}.csv",
                axis=1).tolist()
            links.extend(archive_urls)

        log.info(f"Fetching data...")
        with ThreadPoolExecutor(max_workers=int(len(links) / 10)) as executor:
            response_csv_data = list(executor.map(fetch_community_data, links))
            log.info("Done fetching data")
            return concat(response_csv_data, ignore_index=True)

    except Exception as e:
        log.error(e)

def authenticate():
    API_URL: str = os.getenv("MOBILITY_API_URL", "")
    API_ADMIN: str = os.getenv("MOBILITY_API_ADMIN_USERNAME", "")
    API_PASSWORD: str = os.getenv("MOBILITY_API_ADMIN_PASSWORD", "")

    try:

        form_data = {
            'username': API_ADMIN,
            'password': API_PASSWORD
        }
        auth_response = requests.post(f'{API_URL}/auth/token', data=form_data)
        auth_response.raise_for_status()
        token = auth_response.json().get('access_token')
        headers = {"Authorization": f"Bearer {token}"}

        return {'url': API_URL, 'auth': headers}

    except Exception as e:
        log.error(e)


def run_data_crawling(data_url: str, archive_url: str = None,
                      start_date: datetime.date = None, end_date: datetime.date = None) -> None:
    """
    Runs the data crawling process for current and/or archived data.

    @param data_url: The URL to fetch current data.
    @param archive_url: The URL to fetch archived data (optional).
    @param start_date: The start date for archived data retrieval (optional).
    @param end_date: The end date for archived data retrieval (optional).
    """
    log.info("Authenticating")
    api_data = authenticate()

    log.info("Fechting community data")
    response_data = fetch_community_data(data_url)
    if response_data is None or not len(response_data):
        return

    log.info("Init sensors datastreams")
    sensors, datastreams = initialize_sensors_datastreams(api_data, response_data)

    if archive_url:
        log.info("Getting archive data")
        response_data = get_archive_data(start_date, end_date, response_data[['sensor_id', 'sensor_type']],
                                         archive_url)

    log.info("Creating measurements df")
    new_measurements = create_measurements_df(response_data, datastreams, sensors)

    log.info("Creating measurements at endpoint")

    # chunk dataframe if it is too large
    if len(new_measurements) >= 1e6:
        chunk_size = 100000

        for i in range(0, len(new_measurements), chunk_size):
            df_chunk = new_measurements[i:i+chunk_size]
            create_new_data('measurements', df_chunk, api_data)

    else:
        create_new_data('measurements', new_measurements, api_data)

    log.info("Done creating measurements at endpoint")


def main() -> None:
    """
    Main function to run the data crawling process.
    """
    start_date_str = os.getenv("START_DATE", "20230101")
    end_date_str = os.getenv("END_DATE")
    date_format = "%Y%m%d"

    start_date = datetime.strptime(start_date_str, date_format).date()

    if end_date_str:
        end_date = datetime.strptime(end_date_str, date_format).date()
        if end_date == datetime.now().date():
            end_date = end_date - timedelta(days=1)
    else:
        end_date = (datetime.now() - timedelta(days=1)).date()

    longitude = os.getenv("LONGITUDE", "50.775555")
    latitude = os.getenv("LATITUDE", "6.083611")
    radius_km = os.getenv("RADIUS", "10")

    data_url = os.getenv("SENSOR_COMMUNITY_DATA", "https://data.sensor.community/airrohr/v1/filter/")
    data_url = f'{data_url}area={longitude},{latitude},{radius_km}'
    archive_url = os.getenv("SENSOR_COMMUNITY_ARCHIVE", "https://archive.sensor.community/")

    update_data_from_archive = os.getenv("SENSOR_COMMUNITY_HISTORY", "False").lower() == 'true'

    if update_data_from_archive and start_date < end_date:
        log.info("Crawling from archive")
        run_data_crawling(data_url, archive_url, start_date, end_date)
    else:
        log.info("live crawling")
        run_data_crawling(data_url)


if __name__ == "__main__":

    if logging_level_str == "INFO":
        logging_level = logging.INFO
    elif logging_level_str == "WARNING":
        logging_level = logging.WARNING
    elif logging_level_str == "ERROR":
        logging_level = logging.ERROR

    basicConfig(
        level=logging_level,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S')

    load_dotenv()

    while True:

        start_time = time.time()

        main()

        end_time = time.time()

        time_to_next_crawling = 300 - (end_time - start_time)

        log.info(f"Retrieving new data in {time_to_next_crawling} seconds")
        time.sleep(time_to_next_crawling)
