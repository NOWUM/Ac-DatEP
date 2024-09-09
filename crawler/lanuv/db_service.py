import os
import logging
import time
from typing import Dict

import psycopg2
import psycopg2.extras
import pandas as pd
import shapely


db_name = os.getenv("MOBILITY_DB_NAME")
db_host = os.getenv("MOBILITY_DB_SERVER")
db_port = os.getenv("MOBILITY_DB_PORT")
db_username = os.getenv("MOBILITY_DB_USERNAME")
db_password = os.getenv("MOBILITY_DB_PASSWORD")

DB_URI = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"

def connect() -> psycopg2.extensions.connection | int:
    """
    Function for connection to database.

    Returns:
    ----------
        psycopg2.extensions.connection if succesful or -1 if not.
    """

    logging.info("Trying to connect to database")

    # try to connect for 5 times
    for attempt in range(5):
        try:
            con = psycopg2.connect(
                dbname=db_name,
                user=db_username,
                password=db_password,
                host=db_host,
                port=db_port)
            logging.info("Successfully connected to database")
            return con

        except:
            logging.warning(
                f"Could not connect to database on attempt {attempt + 1}")
            time.sleep(5)
            pass

    return -1

def sensors_exist() -> bool:
    """
    Checks if sensors for LANUV data with ex_ids "AABU" and "VACW" exist.

    Returns:
    ----------
        bool: True if both exist, False if not
    """

    logging.info("Checking if LANUV sensors exist")

    sql = f"""
        SELECT
            *
        FROM
            sensors
        WHERE
            source = 'LANUV'
        AND
            description IN ('Aachen-Burtscheid', 'Aachen Wilhelmstraße')
    """
    result = pd.read_sql(sql, DB_URI)

    # there need to be two sensors with ex_id "AABU" and "VACW"
    if sorted(result["description"].values) != [
        "Aachen Wilhelmstraße",
        "Aachen-Burtscheid"]:
        return False
    else:
        return True

def create_sensors(
        con: psycopg2.extensions.connection) -> Dict[str, int]:
    
    """
    Creates sensors in database

    Parameters:
    ----------
        con: psycopg2.extensions.connection
            Connection to the database

    Returns:
    ----------
        Dict[str, int] containing the station-names: sensor_id pairs
    """
    
    logging.info("Creating sensors")

    point_burtscheid = shapely.Point([50.75473752425752, 6.093892118595028])
    point_wilhelmstrasse = shapely.Point([50.77312781748374, 6.095763792588302])

    sql = f"""
        INSERT INTO sensors (
            source,
            ex_id,
            description,
            geometry,
            longitude,
            latitude,
            confidential)
        VALUES
            ('LANUV', -1, 'Aachen-Burtscheid', '{shapely.to_wkt(point_burtscheid)}', {point_burtscheid.x}, {point_burtscheid.y}, {False}),
            ('LANUV', -2, 'Aachen Wilhelmstraße', '{shapely.to_wkt(point_wilhelmstrasse)}', {point_wilhelmstrasse.x}, {point_wilhelmstrasse.y}, {False})
        RETURNING
            id, description
    """

    cur = con.cursor()
    cur.execute(sql)

    logging.info("Successfully created new sensors")

    result = cur.fetchall()

    return {name: sensor_id for sensor_id, name in result}

def get_datastream_ids() -> pd.DataFrame:
    
    """
    Retrieves datastream_id, sensor_id, type, source and station name for LANUV data.

    Parameters:
    ----------
        con: psycopg2.extension.connection
            Connection to the database

    Returns:
    ----------
        pd.DataFrame containing the data
    """
    
    sql = f"""
        SELECT
            datastreams.id AS ds_id,
            sensors.id AS sensor_id,
            type,
            source,
            description
        FROM
            datastreams
        LEFT JOIN
            sensors
        ON
            datastreams.sensor_id = sensors.id
        WHERE
            sensors.source = 'LANUV'
    """

    return pd.read_sql(sql, DB_URI)

def create_datastreams(
        con: psycopg2.extensions.connection,
        sensor_ids: Dict[str, int]) -> None:
    
    """
    Creates datastreams in database and returns IDs with station name, type and sensor_ids
    in pd.DataFrame.

    Parameters:
    ----------
        con: psycopg2.extensions.connection
            Connection to the database

        sensor_ids: Dict[str, int]
            Dictionary containing station-names: sensor_id pairs    
    """

    logging.info("Creating datastreams")

    # define types
    types = ["Ozon", "SO2", "NO2", "PM10"]

    try:
        # iterate over stations
        for name, sensor_id in sensor_ids.items():

            # iterate over types (Ozon, SO2, ...)
            for _type in types:
                sql = f"""
                    INSERT INTO datastreams (
                        sensor_id,
                        ex_id,
                        type,
                        unit,
                        confidential)
                    VALUES (
                        {sensor_id}, -1, '{_type}', 'µg/m³', {False})
                    RETURNING
                        id
                """
                cur = con.cursor()
                cur.execute(sql)
               
        logging.info("Successfully created datastreams")
    
    except Exception as e:
        # no need to do anything when error appears, as there is something wrong with the code
        # there sould be some kind of auto-mailer here, but this would be out of scope
        logging.error(e)
        raise e

def write_to_database(
    con: psycopg2.extensions.connection,
    measurements: pd.DataFrame) -> None | int:

    logging.info("Writing to database")

    try:
        measurements_data = []

        for idx, row in measurements.iterrows():
            measurements_data.append(tuple(row))

        sql = """
            INSERT INTO measurements (datastream_id, timestamp, value, confidential)
            VALUES %s
        """
        cur = con.cursor()
        psycopg2.extras.execute_values(cur, sql, measurements_data)

        logging.info("Inserted measurements into database")

    except psycopg2.errors.UniqueViolation:
        pass

    except Exception as e:
        logging.error(e)
        raise e
