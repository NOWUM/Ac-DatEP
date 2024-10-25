import db_service

import os
import logging
import time
import schedule

import pandas as pd


LANUV_STATIONS = os.getenv("LANUV_STATIONS", ["VACW", "AABU"])
logging_level_str = os.getenv("LOGGING_LEVEL", "ERROR")


def request_values() -> pd.DataFrame | int:
    """
    Function for requesting values for current air quality.

    Returns:
    ----------
        pd.DataFrame if successful or -1 if not.
    """

    logging.info("Trying to request values from LANUV")

    url = "https://www.lanuv.nrw.de/fileadmin/lanuv/luft/immissionen/aktluftqual/eu_luftqualitaet.csv"

    for attempt in range(5):
        try:
            values_df = pd.read_csv(
                filepath_or_buffer=url,
                delimiter=";",
                encoding="cp1250",
                skiprows=2,
                header=None)
            logging.info("Request successful")
            return values_df

        except:
            logging.warning(
                f"Could retrieve values on attempt {attempt + 1}")
            time.sleep(5)
            pass

    logging.error("Could not retrieve values in 5 tries")
    return -1


def request_columns() -> pd.DataFrame | int:
    """
    Function for requesting columns for current air quality file.

    Returns:
    ----------
        pd.DataFrame if successful or -1 if not.
    """

    logging.info("Trying to request columns from LANUV")

    url = "https://www.lanuv.nrw.de/fileadmin/lanuv/luft/immissionen/aktluftqual/header_eu_luftqualitaet.csv"

    for attempt in range(5):
        try:
            values_df = pd.read_csv(
                filepath_or_buffer=url,
                delimiter=";",
                encoding="cp1250",
                comment="#")
            logging.info("Request successful")
            return values_df

        except:
            logging.warning(
                f"Could retrieve columns on attempt {attempt + 1}")
            time.sleep(5)
            pass

    logging.error("Could not retrieve columns in 5 tries")
    return -1


def clean_data(
        values_df: pd.DataFrame,
        columns_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans retrieved data and adds columns.

    Returns:
    ----------
        pd.DataFrame: Cleaned data
    """

    logging.info("Cleaning retrieved data")

    try:
        # drop last (empty) column
        values_df.drop(columns=6, inplace=True)

        # set columns
        values_df.columns = columns_df.columns
        values_df.rename(columns={"Station": "description"}, inplace=True)

        # filter for stations in Aachen
        condition = values_df["Kürzel"].isin(LANUV_STATIONS)
        values_df = values_df[condition].reset_index(drop=True)

        # replace missing values and convert to int
        for col in ["Ozon", "SO2", "NO2", "PM10"]:
            values_df[col] = values_df[col].str.replace("<", "")
            values_df[col] = values_df[col].str.replace("-", "-1")
            values_df[col] = values_df[col].str.replace("*", "-1")
            values_df[col] = values_df[col].astype(int)

        # melt data to long format
        values_df = pd.melt(
            frame=values_df,
            id_vars=["description", "Kürzel"],
            value_vars=["Ozon", "SO2", "NO2", "PM10"],
            var_name="type")

        logging.info("Cleaning successful")

        return values_df

    except Exception as e:
        # logging the error and still raising exception because if
        # anything goes wrong here there is no point in trying againg later,
        # the code needs to be changed
        # there should be some kind of auto mail here, but this would be
        # out of scope
        logging.error(e)
        raise e


def create_measurements_df(
        data: pd.DataFrame,
        datastream_ids: pd.DataFrame) -> pd.DataFrame:

    logging.info("Creating measurements DataFrame")

    try:
        # create DataFrame by merging datastream IDs onto data
        measurements_df = pd.merge(
            left=data,
            right=datastream_ids,
            on=["description", "type"],
            how="left")

        # rename column
        measurements_df.rename(
            columns={"ds_id": "datastream_id"}, inplace=True)

        # create timestamp and confidential column
        measurements_df["timestamp"] = pd.Timestamp.now().floor("h")
        measurements_df["confidential"] = False

        # only use datastream ID and value columns
        measurements_df = measurements_df[[
            "datastream_id", "timestamp", "value", "confidential"]]

        logging.info("Successfully created measurements DataFrame")

        return measurements_df

    except Exception as e:
        logging.error(e)
        raise e


def request_and_process():

    # connect to database
    con = db_service.connect()

    # request values from LANUV
    values_df = request_values()

    # request column names from LANUV
    columns_df = request_columns()

    # clean up data
    data = clean_data(
        values_df=values_df,
        columns_df=columns_df)

    # create sensors and datastreams if they dont exist
    if not db_service.sensors_exist():
        sensor_ids = db_service.create_sensors(con)

        db_service.create_datastreams(
            con,
            sensor_ids)

    # retrieve the datastream IDs
    datastream_ids = db_service.get_datastream_ids()

    # creating measurements dataframe
    measurements_df = create_measurements_df(
        data=data,
        datastream_ids=datastream_ids)

    # write measurements to database
    db_service.write_to_database(
        con=con,
        measurements=measurements_df)

    con.commit()
    con.close()


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

    # schedule requesting and processing for every hour
    schedule.every().hour.do(request_and_process)

    # run job once on start up
    schedule.run_all()

    while True:
        schedule.run_pending()
        time.sleep(1)
