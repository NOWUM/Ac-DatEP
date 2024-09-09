import os
import logging
import time

from typing import Tuple, List

import psycopg2
import pandas as pd


db_name = os.getenv("MOBILITY_DB_NAME")
db_host = os.getenv("MOBILITY_DB_SERVER")
db_port = os.getenv("MOBILITY_DB_PORT")
db_username = os.getenv("MOBILITY_DB_USERNAME")
db_password = os.getenv("MOBILITY_DB_PASSWORD")


def connect_to_database() -> psycopg2.extensions.connection | int:

    """
    Attempts 5 times to connect to AC-DatEP database and returns connection or error code.
    """

    logging.info("Trying to connect to database...")

    URI = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"

    for attempt in range(5):
        try:
            con = psycopg2.connect(URI)
            logging.info("Connected to DB")
            return con
        except Exception as e:
            logging.warning(f"Could not connect to DB on attempt {attempt}: {e}")
            time.sleep(1)
            continue

    logging.error("Could not connect to database in 5 attempts, aborting")
    return -1


def create_table(con: psycopg2.extensions.connection) -> None | int:

    """
    Creates table in AC-DatEP database or returns error code.
    """

    logging.info("Creating table if needed")

    sql = f"""
        CREATE TABLE IF NOT EXISTS events (
            event_name TEXT,
            date_from DATE,
            date_to DATE,
            additional_info TEXT,
            confidential BOOL)
    """

    try:
        cur = con.cursor()
        cur.execute(sql)
        logging.info("Successfully created table (if needed)")
        return 0
    except Exception as e:
        logging.error(f"Something went wrong creating table: {e}")
        return -1


def get_existing_events(con: psycopg2.extensions.connection) -> List[Tuple | None] | int:

    """
    Retrieves existing events from AC-DatEP database or returns error code.
    """

    logging.info("Trying to retrieve existing events from DB")

    try:
        cur = con.cursor()
        cur.execute("SELECT * FROM events")
        logging.info("Successfully retrieved existing events from DB")
        return cur.fetchall()
    except Exception as e:
        logging.error(f"Something went wrong fetching existing events: {e}")
        return -1


def feed_to_database(
        con: psycopg2.extensions.connection,
        data: List[Tuple]) -> None | int:

    """
    Feeds data into AC-DatEP database, table events, or returns error code.

    Parameters:
    -----------------
        con: psycopg2.extensions.connection
            The connection object to the database

        data: List[Tuple]
            The data to be fed into the database
    """

    logging.info("Trying to feed new events into DB")

    try:
        cur = con.cursor()
        cur.executemany("INSERT INTO events VALUES (%s, %s, %s, %s, %s)", data)
        logging.info("Successfully fed new events into DB")
    except Exception as e:
        logging.error(f"Something went wrong feeding data into database: {e}")
        return -1
