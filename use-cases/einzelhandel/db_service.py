import os
import logging
import pandas as pd
from copy import deepcopy
from datetime import datetime

import psycopg2
import psycopg2.extras

def connect() -> psycopg2.extensions.connection:

    # read connection parameters
    host = os.getenv("MOBILITY_DB_SERVER")
    port = os.getenv("MOBILITY_DB_PORT")
    name = os.getenv("MOBILITY_DB_NAME")
    user = os.getenv("MOBILITY_DB_USERNAME")
    password = os.getenv("MOBILITY_DB_PASSWORD")

    uri = f"postgresql://{user}:{password}@{host}:{port}/{name}"

    logging.info("Trying to connect to database")

    # try 5 times to connect
    for i in range(1, 6):
        try:
            con = psycopg2.connect(uri)
            logging.info("Succesfully connected to database")
            return con

        except Exception as e:
            # log warning on attempt 1 to 4
            if i < 5:
                msg = f"Could not connect to DB on attempt {i}"
                logging.warning(msg)

            # log error after attempt 5 and return None
            else:
                raise Exception(f"Connection to database failed! URI: {uri}")


def convert_to_pydatetime(orig_timestamp: str):

    try:
        timestamp = datetime.strptime(
            orig_timestamp,
            "%Y-%m-%dT%H:%M:%S.%f%z")
        timestamp = timestamp.replace(tzinfo=None)

    except Exception as e:
        msg = f"Could not convert timestamp {orig_timestamp} to datetime: {e}"
        logging.error(msg)
        return 0
    return timestamp


def check_sensor_existence(
        con: psycopg2.extensions.connection,
        ex_id: str,
        source: str) -> bool:

    sql = f"""
        SELECT id
        FROM sensors
        WHERE ex_id = '{ex_id}'
        AND source = '{source}'
    """

    msg = f"Trying to check sensor existence for "
    msg += f"ex_id {ex_id} and source {source}"
    logging.info(msg)

    try:
        cur = con.cursor()
        cur.execute(sql)

        result = cur.fetchone()

        logging.info("Succesfully checked sensors existence")

        # sensor does not exist if result is None
        if result == None:
            logging.info(f"Sensor with ex_id {ex_id} does not exist")
            return False

        # return the ID otherwise
        else:
            logging.info(f"Sensor with ex_id {ex_id} does exist")
            return result[0]

    except Exception as e:
        con.rollback()
        msg = f"Something went wrong: {e}"
        logging.error(msg)
        return -1

def load_ds_measurements(con: psycopg2.extensions.connection, ds_id: int, start_time: str, end_time: str):
    """
    Loads measurements from one datastream in a specific timeframe.

    Parameters:
    ----------
        ds_id: datastream id
        start_time: first timestamp
        end_time: last timestamp

    Returns:
    ----------
        bool: True if both exist, False if not
    """
    sql = f"""
        SELECT *
        FROM measurements
        WHERE datastream_id = {ds_id}
        AND timestamp > '{start_time}'
        AND timestamp < '{end_time}'
    """
    try: 
        cur = con.cursor()
        result = pd.read_sql(sql, con)
        return result
    except Exception as e:
        con.rollback()
        msg = f"Something went wrong: {e}"
        logging.error(msg)
        return -1
    
## Get Location for datastream
def load_ds_lonlat(con: psycopg2.extensions.connection, ds_id: int):
    """
    Loads measurements from one datastream in a specific timeframe.

    Parameters:
    ----------
        ds_id: datastream id
        start_time: first timestamp
        end_time: last timestamp

    Returns:
    ----------
        bool: True if both exist, False if not
    """
    sql = f"""
        SELECT sensors.longitude, sensors.latitude
        FROM sensors
        JOIN datastreams
        ON sensors.id = datastreams.sensor_id 
        WHERE datastreams.id = {ds_id}
    """
    try: 
        result = pd.read_sql(sql, con)
        return result
    except Exception as e:
        con.rollback()
        msg = f"Something went wrong: {e}"
        logging.error(msg)
        return -1

def check_datastream_existence(
        con: psycopg2.extensions.connection,
        sensor_id: int,
        _type: str):

    sql = f"""
        SELECT id
        FROM datastreams
        WHERE sensor_id = {sensor_id}
        AND type = '{_type}'
    """

    msg = f"Trying to check datastream existence for "
    msg += f"sensor id {sensor_id} and type {_type}"
    logging.info(msg)

    try:
        cur = con.cursor()
        cur.execute(sql)

        result = cur.fetchone()

        logging.info("Succesfully checked datastream existence")

        if result == None:
            logging.info(f"Datastream with sensor_id {sensor_id} and type {_type} does not exist")
            return False

        else:
            logging.info(f"Datastream with sensor_id {sensor_id} and type {_type} does exist")
            return result[0]

    except Exception as e:
        con.rollback()
        msg = f"Something went wrong: {e}"
        logging.error(msg)
        return -1