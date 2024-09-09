import os
import logging
from datetime import datetime
import time

import pandas as pd
import requests
import psycopg2

import db_service


app_id = os.getenv("INRIX_APP_ID")
hash_token = os.getenv("INRIX_HASH_TOKEN")

logging_level_str = os.getenv("LOGGING_LEVEL", "ERROR")

# remove warning for line 115
pd.set_option("future.no_silent_downcasting", True)


class InrixCrawler:
    """
    Class for fetching data from Inrix Database and writing it to NOWUM Timescale DB
    """

    def __init__(self) -> None:
        self.app_id = app_id
        self.hash_token = hash_token

    def _send_request(
            self,
            url: str) -> requests.models.Response | None:
        """
        Function for handling sending of get requests

        Parameters:
        -----------
            url: str
                URL to send request to
        
        Returns:
        -----------
            r: requests.models.Response
                Response from server
        """

        try:
            r = requests.get(url)
            r.raise_for_status()
        except Exception as e:
            msg = f"Something went wrong requesting something: {e}"
            logging.error(msg)

            return -1

        return r

    def get_api_token(self) -> str | int:
        """
        Sends GET-Request with appID and hashToken to INREX API and returns the API token

        Returns:
        -----------
            api_token: str
                The INREX API token
        """

        logging.info("Fechting INRIX API token...")

        # build the URL
        url = "https://uas-api.inrix.com/v1/appToken?appId="
        url += self.app_id
        url += "&hashToken="
        url += self.hash_token

        # send get request
        r = self._send_request(url)

        # extract API token from response or return error code
        if r == -1:
            return -1
        else:
            return self._extract_api_token(r)

    def _extract_api_token(
            self,
            r: requests.models.Response) -> str | int:

        try:
            return r.json()["result"]["token"]
        except Exception as e:
            msg = f"Response has unexpected format: {e}"
            logging.error(msg)

            return -1

    def __clean_speed_segments_data(
            self,
            data: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans speed segment data

        Parameters:
        -----------
            data: pd.DataFrame
                DataFrame containing the speed segment data
        
        Returns:
        -----------
            data: pd.DataFrame
                DataFrame containing the cleaned speed segment data
        """

        # only if segment is closed data will contain True
        # otherwise None, so we fill it with False
        data.fillna({"segmentClosed": False}, inplace=True)

        # we need the segmentClosed column as double precision, so we cast it
        data["segmentClosed"] = data["segmentClosed"].astype(float)

        # rename 'code' column to 'XDSegID' and cast to int
        data.rename(columns={"code": "XDSegID"}, inplace=True)
        data["XDSegID"] = data["XDSegID"].astype(int)

        # fill missing speeds (where segment is closed) with 0
        data.fillna({"speed": 0}, inplace=True)

        # set timestamps to UTC
        data["timestamp"] = data["timestamp"].dt.tz_localize("UTC")

        return data

    def get_speed_segments(
            self,
            api_token: str,
            northwest_lat: float | str = 50.8061702,
            northwest_lon: float | str = 6.0530048,
            southeast_lat: float | str = 50.7414927,
            southeast_lon: float | str = 6.1705204) -> pd.DataFrame | int:
        """
        Function for fetching speed segments in given box. More regarding API call:
        https://docs.inrix.com/traffic/segmentspeed/

        Parameters:
        -----------
            northwest_lat: float | str
                Latitude of northwest corner of the box
            northwest_lon: float | str
                Longitude of northwest corner of the box
            southeast_lat: float | str
                Latitude of southeast corner of the box
            southeast_lon: float | str
                Longitude of southeast corner of the box

        Returns:
        -----------
            df: pd.DataFrame
                DataFrame containing the retrieved data
        """

        logging.info("Fetching speed segment data...")

        # build URL
        url = f"https://segment-api.inrix.com/v1/segments/"
        url += f"speed?box={northwest_lat}|{northwest_lon},{southeast_lat}|{southeast_lon}"
        url += f"&units=1"
        url += f"&SpeedOutputFields=All"
        url += f"&accesstoken={api_token}"

        # send get request
        r = self._send_request(url)

        # extract the data
        try:
            data = r.json()["result"]["segmentspeeds"][0]["segments"]
        except Exception as e:
            msg = f"Response has unexpected format: {e}"
            logging.error(msg)

            return -1

        # extract the time
        try:
            raw_timestring = r.json()["result"]["segmentspeeds"][0]["time"]
            timestamp = datetime.strptime(raw_timestring, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError as e:
            msg = f"Provided time in server response has unexpected format: {e}"
            logging.error(msg)

            return -1

        # convert data to DataFrame
        try:
            data = pd.json_normalize(data)
            data["timestamp"] = pd.to_datetime(timestamp)
        except Exception as e:
            msg = f"Something went wrong converting received data to DataFrame: {e}"
            logging.error(msg)

            return -1

        # clean data
        try:
            return self.__clean_speed_segments_data(data)
        except Exception as e:
            msg = f"Something went wrong cleaning speed segment data: {e}"
            logging.error(msg)

            return -1

    def create_measurements_dataframe(
            self,
            speed_segment_data: pd.DataFrame,
            inrix_sensors_ids: pd.DataFrame) -> pd.DataFrame | None:

        # convert speed segment data to long format for merging
        speed_segment_data = pd.melt(
            frame=speed_segment_data,
            id_vars=["XDSegID", "timestamp"],
            value_vars=[
                "speed",
                "average",
                "segmentClosed",
                "reference",
                "travelTimeMinutes",
                "speedBucket"],
            var_name="type")

        # merge sensor_ids onto speed segment data
        data = pd.merge(
            left=speed_segment_data,
            right=inrix_sensors_ids,
            left_on="XDSegID",
            right_on="ex_id",
            how="left")

        # match database type names
        data["type"] = data["type"].map({
            "average": "average speed",
            "segmentClosed": "segment closed",
            "speed": "speed",
            "reference": "reference speed",
            "travelTimeMinutes": "travel time",
            "speedBucket": "level of congestion"})

        # remove datapoints where there is no sensor
        data.dropna(subset="sensor_id", inplace=True)

        # get the datastreams data is fetched for
        sensor_ids = tuple(int(_) for _ in data["sensor_id"].values)
        datastream_ids = db_service.get_datastream_ids(
            sensor_ids=sensor_ids)

        if not isinstance(datastream_ids, pd.DataFrame) and not datastream_ids:
            return None

        # merge datastream ids onto data
        measurements = pd.merge(
            left=data,
            right=datastream_ids,
            on=["sensor_id", "type"],
            how="left")
        measurements = measurements[["id", "timestamp", "value"]]

        # fill missing measurements with -1
        measurements.fillna({"value": -1}, inplace=True)

        # new column with "confidential" value set to True
        measurements["confidential"] = True

        return measurements

    def write_to_database(
            self,
            con: psycopg2.extensions.connection,
            speed_segment_data: pd.DataFrame):

        # get the sensors that don't exists and need to be created
        XDSegIDs = tuple(str(_) for _ in speed_segment_data["XDSegID"].values)
        sensors_to_create = db_service.get_sensors_to_create(
            con=con,
            XDSegIDs=XDSegIDs)

        # if new sensors need to be created
        # create the sensors and datastreams
        if sensors_to_create:
            new_sensors = db_service.create_sensors(
                con=con,
                sensors_to_create=sensors_to_create)

            db_service.create_datastreams(
                con=con,
                new_sensors=new_sensors)

        # get the sensors data is fetched for
        inrix_sensors_ids = db_service.get_sensor_ids(
            ex_ids=XDSegIDs)

        measurements = self.create_measurements_dataframe(
            speed_segment_data=speed_segment_data,
            inrix_sensors_ids=inrix_sensors_ids)

        if not isinstance(measurements, pd.DataFrame) and measurements == -1:
            logging.error(f"Could not create measurements DF")
            return -1

        return db_service.write_measurements_to_database(
            con=con,
            measurements=measurements)


if __name__ == "__main__":

    if logging_level_str == "INFO":
        logging_level = logging.INFO
    elif logging_level_str == "WARNING":
        logging_level = logging.WARNING
    elif logging_level_str == "ERROR":
        logging_level = logging.ERROR

    # logging
    logging.basicConfig(
        level=logging_level,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S')

    while True:

        start_time = time.time()

        # connect to database
        con = db_service.connect()

        # stop script if DB connection could not be established
        if not con:
            break

        # check if table "inrix" exists, as otherwise there is no way of
        # matching inrix segment IDs to actual street names or coordinates
        if not db_service.xds_table_exists(con):
            msg = f"INRIX XDS information table does not exist "
            msg += f"but is needed for matching XDSegmentIDs to "
            msg += f"street names and coordinates. Please request "
            msg += f"XDSegment information from INRIX and write "
            msg += f"to DB using 'xds_information.py' file."
            logging.error(msg)
            break

        # Inrix crawler instance
        inrix_crawler = InrixCrawler()

        # fetch API token from INRIX
        api_token = inrix_crawler.get_api_token()

        i = 1
        while i < 5 and api_token == -1:
            api_token = inrix_crawler.get_api_token()
            i += 1

        if api_token == -1:
            break

        # fetching speed segments data
        speed_segment_data = inrix_crawler.get_speed_segments(api_token)

        if not isinstance(speed_segment_data, pd.DataFrame) and speed_segment_data == -1:
            break

        # writing to database
        if inrix_crawler.write_to_database(con, speed_segment_data):
            break

        # commit and close conncetion
        con.commit()
        logging.info("Commited measurements to database")

        # new call to API 2 minutes after last call
        elapsed_time = time.time() - start_time
        if elapsed_time > 120:
            elapsed_time = 120.0
        logging.info(
            f"Requesting new data in {120 - elapsed_time:4.4} seconds")
        time.sleep(120 - elapsed_time)
