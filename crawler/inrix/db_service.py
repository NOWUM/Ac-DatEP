import os
import logging
from typing import Tuple, List

import pandas as pd
import psycopg2
import psycopg2.extras


db_name = os.getenv("MOBILITY_DB_NAME")
db_host = os.getenv("MOBILITY_DB_SERVER")
db_port = os.getenv("MOBILITY_DB_PORT")
db_username = os.getenv("MOBILITY_DB_USERNAME")
db_password = os.getenv("MOBILITY_DB_PASSWORD")


def connect():
    """
    Creates connection to AC-DatEp database.

    Returns:
    --------------
        con: psycopg2.extensions.connection
            Connection to the database
    """

    logging.info("Trying to create database connection...")

    URI = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
    try:
        return psycopg2.connect(URI)

    except Exception as e:
        # log error
        msg = f"Database connection could not be established: {e}\n"
        logging.error(msg)

        return None


def xds_table_exists(
        con: psycopg2.extensions.connection):

    cur = con.cursor()

    stmt = """SELECT EXISTS (
        SELECT FROM
            pg_tables
        WHERE
            schemaname = 'public' AND
            tablename  = 'inrix');"""

    cur.execute(stmt)
    return cur.fetchone()[0]



def get_sensors_to_create(
        con: psycopg2.extensions.connection,
        XDSegIDs: tuple) -> Tuple[int] | None:
    """
    Filters out XDSegIDs from list of IDs which already have a
    sensor and returns only XDSegIDs that need sensor creation:

    Parameters:
    --------------
        con: psycopg2.extensions.connection
            Psycopg2 connection to DB

        XDSegIDs: tuple[int]
            Tuple with XDSegIDs as integer

    Returns:
    --------------
        tuple[int] | None
            List with XDSegIDs which need have a sensor created or
            None if no new sensor need to be created
    """

    logging.info("Fetching sensors that need to be created...")

    # query ex_ids of sensors that are
    # in given XDSegIDs
    # and have source 'INRIX'
    if len(XDSegIDs) == 1:
        where_clause_stmt = f"= {XDSegIDs[0]}"
    else:
        where_clause_stmt = f"IN {XDSegIDs}"

    sql = f"""
    SELECT
        ex_id
    FROM
        sensors
    WHERE
        ex_id {where_clause_stmt}
    AND
        source = 'INRIX'
    """
    cur = con.cursor()
    cur.execute(sql)
    res = [x[0] for x in cur.fetchall()]

    # filter
    sensors_to_be_created = tuple(x for x in XDSegIDs if x not in res)

    if sensors_to_be_created == ():
        return None
    else:
        return sensors_to_be_created


def create_sensors(
        con: psycopg2.extensions.connection,
        sensors_to_create: Tuple[int]) -> List[Tuple[int, int]]:
    """
    Creates sensors for INRIX data in sensors table for given XDSegIDs.

    Parameters:
    --------------
        con: psycopg2.extensions.connection
            Psycopg2 connection to DB

        sensors_to_create: Tuple[int]
            Tuple with XDSegIDs to create sensors for

    Returns:
    --------------
        List[Tuple[int]]:
            IDs and ex_ids (XDSegIDs) for newly created sensors
    """

    logging.info("Creating sensors...")

    if len(sensors_to_create) == 1:
        where_clause_stmt = f"= {sensors_to_create[0]}"
    else:
        where_clause_stmt = f"IN {sensors_to_create}"

    sql = f"""
        INSERT INTO
            sensors (source, ex_id, description, geometry, longitude, latitude, confidential)
        SELECT
            'INRIX', "XDSegID", 'INRIX Speed Segment', inrix.geometry, inrix.longitude, inrix.latitude, {True}
        FROM
            inrix
        WHERE
            "XDSegID" {where_clause_stmt}
        RETURNING
            id, ex_id
        """

    try:
        cur = con.cursor()
        cur.execute(sql)
    except Exception as e:
        msg = f"Something went wrong creating sensors in database: {e}"
        logging.error(msg)

    return cur.fetchall()


def create_datastreams(
        con: psycopg2.extensions.connection,
        new_sensors: List[Tuple[int, int]]) -> None:

    logging.info("Creating datastreams for new sensors...")

    # create 'extra' data, e. g. type and unit
    new_sensor_data = create_datastream_data(new_sensors)

    sql = f"""
        INSERT INTO
            datastreams (sensor_id, ex_id, type, unit, confidential)
        VALUES
            %s
    """

    try:
        cur = con.cursor()
        psycopg2.extras.execute_values(cur, sql, new_sensor_data)
    except Exception as e:
        msg = f"Something went wrong creating datastreams: {e}"

        logging.warning(msg)


def create_datastream_data(new_sensors):
    """
    Creates 'extra' data, e. g. type and unit for datastream creation.

    Parameters:
        new_sensors: List[Tuple[int, int]]
            List of tuples with sensor ID and ex_id (XDSegID)

    Returns:
        new_sensor_data: List[Tuple[int, int, int, str, str]]
            The 'extra' data for the datastreams
    """

    new_sensor_data = []

    for new_sensor in new_sensors:
        sensor_id = new_sensor[0]
        ex_id = -1
        confidential = True

        for _type, unit in zip(
            [
                "speed",
                "average speed",
                "segment closed",
                "reference speed",
                "travel time",
                "level of congestion"],
            [
                "km/h",
                "km/h",
                "None",
                "km/h",
                "minutes",
                "None"]):
            new_sensor_data.append((sensor_id, ex_id, _type, unit, confidential))

    return new_sensor_data


def get_sensor_ids(
        ex_ids: Tuple) -> pd.DataFrame | None:
    """
    Returns sensor ID for external ID and source combination.

    Parameters:
    --------------
        con: psycopg2.extensions.connection
            The database connection object.
        ex_id: tuple[int]
            Tuple with external sensor IDs.

    Returns:
    --------------
        sensor_id: int
            The internal sensor ID.

    Raises:
    --------------
        ValueError:
            Raises ValueError if external_ID and source combination
            does not exists.
    """

    logging.info("Fetching sensor IDs...")

    if len(ex_ids) == 1:
        where_clause_stmt = f"= {ex_ids[0]}"
    else:
        where_clause_stmt = f"IN {ex_ids}"

    # execute query
    sql = f"""
    SELECT
        id AS sensor_id,
        ex_id::int
    FROM
        sensors
    WHERE
        ex_id {where_clause_stmt}
    AND
        source = 'INRIX'
    """

    try:
        return pd.read_sql(
            sql=sql,
            con=f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")
    except Exception as e:
        msg = f"Something went wrong getting IDs of sensors: {e}"
        logging.error(msg)

        return None


def get_datastream_ids(
        sensor_ids: Tuple[int]) -> pd.DataFrame | None:
    """
    Gets the datastream IDs for a given sensor.

    Parameters:
    --------------
        con: psycopg2.extensions.connection
            The database connection object.

        sensor_id: int
            ID of the sensor to get datastream IDs for.

    Returns:
    --------------
        datastream_ids: Dict[str, int|bool]
            Dictionary with datastream.type as key and
            datastream.id as value
    """

    logging.info("Fetching datastream IDs...")

    sql = f"""
        SELECT
            *
        FROM
            datastreams
        WHERE
            sensor_id IN {sensor_ids}
    """

    try:
        return pd.read_sql(
            sql=sql,
            con=f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")
    except Exception as e:
        msg = f"Something went wrong getting IDs of datastreams: {e}"
        logging.error(msg)

        return None


def write_measurements_to_database(
        con: psycopg2.extensions.connection,
        measurements: pd.DataFrame) -> None | int:
    """
    Writes INRIX data to database.

    Parameters:
    --------------
        con: psycopg2.extensions.connection
            The database connection object.

        measurements: pd.DataFrame
            pandas DataFrame containing timestamp, value and id column

    Returns:
    --------------
        None
    """

    logging.info("Writing measurements to database...")

    cur = con.cursor()

    sql = f"""
    INSERT INTO
        measurements (datastream_id, timestamp, value, confidential)
    VALUES
        %s
    """

    measurements_data = []
    for idx, row in measurements.iterrows():
        measurements_data.append(tuple(row))

    try:
        psycopg2.extras.execute_values(cur, sql, measurements_data)
    except Exception as e:
        msg = f"Something went wrong writing measurements to database: {e}"
        logging.error(msg)

        return -1
