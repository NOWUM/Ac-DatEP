import os
import logging
from copy import deepcopy
from datetime import datetime

import psycopg2
import psycopg2.extras


BOXES_DATABASE_TYPE_DICT = {
    "hum": "humidity",
    "blu": "bluetooth",
    "ble": "bluetooth",
    "temp": "temperature",
    "wifi": "wifi",
    "co2": "CO2",
    "pm10": "PM10",
    "pm25": "PM2.5",
    "vehicles": "motor traffic measurement",
    "smallvehicle": "small vehicles measurement",
    "bigvehicle": "big vehicles measurement"}

BOXES_DATABASE_UNIT_DICT = {
    "CO2": "µg/m³",
    "PM10": "µg/m³",
    "PM2.5": "µg/m³",
    "wifi": "connections counted",
    "bluetooth": "connections counted",
    "temperature": "°C",
    "humidity": "%",
    "loudness": "dB",
    "motor traffic measurement": "Vehicles counted",
    "small vehicles measurement": "Vehicles counted",
    "big vehicles measurement": "Vehicles counted"}


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
            logging.info(f"Succesfully connected to database {name}")
            return con

        except Exception as e:
            # log warning on attempt 1 to 4
            if i < 5:
                msg = f"Could not connect to DB on attempt {i}"
                logging.warning(msg)

            # log error after attempt 5 and return None
            else:
                msg = f"DB connection could not be established "
                msg += f"after 5 attempts. Not processing payload. Reason: {e}"
                logging.error(msg)
                return -1


def process_payload(payload: dict):

    # start by fetching metadata (sensor ID, lat-lon, ...)
    # and data (temperature, humidity, ...) from payload
    data, metadata = fetch_data_metadata_from_payload(payload)

    # exit if data or device ID is missing
    if data == -1 or metadata == -1:
        return 0

    # exit if device ID does not contain "wilson"
    if "wilson" not in metadata["device_id"]:
        logging.info("Not a 'wilson' sensor - skipping")
        return 0

    # build connection to database
    con = connect()

    # exit if connection could not be established
    if con == -1:
        return 0

    # iterate over data / measurements
    for key, measurement in data.items():

        if measurement == None:
            logging.warning("No measurement provided - skipping")
            continue

        # convert to DB format
        _type = BOXES_DATABASE_TYPE_DICT.get(key, key)
        unit = BOXES_DATABASE_UNIT_DICT.get(_type, "")

        # skip everything which is not wifi or bluetooth connections
        if _type not in ["wifi", "bluetooth"]:
            logging.info(f"Skipping {_type}")
            continue

        # log warning for unknown datastream type
        if _type not in BOXES_DATABASE_TYPE_DICT.values():
            logging.warning(f"Unknown datastream type: {_type}")

        # check if datastream exists
        datastream_id = check_datastream_existence(
            con=con,
            sensor_id=metadata["device_id"],
            _type=_type)

        # create datastream if it doenst exist
        if not datastream_id:
            datastream_id = create_datastream(
                con=con,
                sensor_id=metadata["device_id"],
                _type=_type,
                unit=unit)
            # exit function if creation failed
            if datastream_id == -1:
                return 0

        # convert timestamp to useable python datetime format
        timestamp = datetime.now().isoformat()

        # insert measurement into table
        insert_measurement(
            con=con,
            datastream_id=datastream_id,
            timestamp=timestamp,
            value=measurement)

    # commit and close connection
    con.commit()
    con.close()


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


def fetch_data_metadata_from_payload(payload: dict) -> tuple[dict, dict]:

    message = deepcopy(payload.get("uplink_message", -1))
    if message == -1:
        logging.warning("No data delivered")
        return -1, -1

    data = deepcopy(message.get("decoded_payload", -1))

    if data == -1:
        logging.warning("No data delivered")
        return -1, -1

    metadata = {}
    metadata["device_id"] = deepcopy(payload["end_device_ids"].get("device_id", -1))

    if metadata["device_id"] == -1:
        logging.warning("No device ID delivered")
        return -1, -1

    return data, metadata


def check_datastream_existence(
        con: psycopg2.extensions.connection,
        sensor_id: int,
        _type: str):

    sql = f"""
        SELECT id
        FROM einzelhandel.datastreams
        WHERE sensor_id = '{sensor_id}'
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


def create_datastream(
        con: psycopg2.extensions.connection,
        sensor_id: int,
        _type: str,
        unit: str):

    msg = f"Trying to create datastream with type {_type} "
    msg += f"for sensor {sensor_id}"
    logging.info(msg)

    sql = f"""
        INSERT INTO einzelhandel.datastreams (
            sensor_id,
            type,
            unit)
        VALUES (
            '{sensor_id}',
            '{_type}',
            '{unit}')
        RETURNING id
    """

    try:
        cur = con.cursor()
        cur.execute(sql)
        result = cur.fetchone()[0]

        logging.info("Succesfully created datastream")

        return result

    except Exception as e:
        con.rollback()
        msg = f"Something went wrong: {e}"
        logging.error(msg)
        return -1


def insert_measurement(
        con: psycopg2.extensions.connection,
        datastream_id: int,
        timestamp: str,
        value: float):

    logging.info("Trying to create measurement...")

    sql = f"""
        INSERT INTO einzelhandel.measurements (
            datastream_id,
            timestamp,
            value)
        VALUES (
            {datastream_id},
            '{timestamp}',
            {value})
    """

    try:
        cur = con.cursor()
        cur.execute(sql)

        logging.info("Succesfully created measurement")

    except Exception as e:
        con.rollback()
        logging.error(e)
        logging.error(f"Datastream_id: {datastream_id}, timestamp: {timestamp}, value: {value}")
        return -1
