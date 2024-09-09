import os
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pytz

import schedule
import psycopg2

from dotenv import load_dotenv
load_dotenv()

host = os.getenv("MOBILITY_DB_SERVER")
port = os.getenv("MOBILITY_DB_PORT")
user = os.getenv("MOBILITY_DB_USERNAME")
password = os.getenv("MOBILITY_DB_PASSWORD")
dbname = os.getenv("MOBILITY_DB_NAME")

DB_URI = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

logging_level_str = os.getenv("LOGGING_LEVEL", "ERROR")

mv_refreshment_time_str = os.getenv("DB_MV_REFRESHMENT_TIME", "300")


def connect_to_db() -> psycopg2.extensions.connection:
    logging.info("Trying to connect to DB")
    try:
        con = psycopg2.connect(DB_URI)
        logging.info("Succesfully connected")
        return con
    except Exception as e:
        logging.error(e)
        return None


def create_comparison_timestamp(
        time_delta: timedelta):

    try:
        timestamp = (datetime.now(tz=pytz.timezone("UTC")) - time_delta)
        timestamp_str = timestamp.strftime("%Y-%m-%d %X")
        return timestamp_str

    except Exception as e:
        logging.error(e)
        return None


def refresh_latest_measurements_mv():

    # connect
    con = connect_to_db()

    if not con:
        return None

    # execute refresh
    logging.info("Trying to refresh materialized view")
    sql = f"REFRESH MATERIALIZED VIEW CONCURRENTLY latest_measurements"
    try:
        cur = con.cursor()
        cur.execute(sql)
    except Exception as e:
        logging.error(e)
        con.rollback()
        return None

    # commit and close connection
    logging.info("Commiting changes and closing connection")
    con.commit()
    con.close()


def create_view_query(
        bucket: str,
        agg: str,
        comparison_timestamp: str):

    viewname = f"""bucketed_measurements_{bucket.replace(" ", "")}_{agg}"""

    sql = f"""
    CREATE MATERIALIZED VIEW {viewname} AS
        SELECT
            time_bucket('{bucket}', timestamp) AS bucket,
            {agg}(value) as value,
            datastream_id
        FROM measurements
        WHERE timestamp >= '{comparison_timestamp}'
        GROUP BY bucket, datastream_id
        ORDER BY bucket, datastream_id ASC
    """

    return sql, viewname


def create_mv(
        bucket: str,
        agg: str,
        time_delta: timedelta):

    con = connect_to_db()

    comparison_timestamp = create_comparison_timestamp(time_delta)

    if not comparison_timestamp:
        logging.error("Could not create timestamp")
        return None

    sql, viewname = create_view_query(bucket, agg, comparison_timestamp)

    try:
        cur = con.cursor()

        # drop old view
        logging.info(f"Trying to drop old view {viewname}")
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {viewname}")
        logging.info(f"Succesfully dropped old view {viewname}")

        # create new view
        logging.info(f"Trying to create new view {viewname}")
        cur.execute(sql)
        logging.info(f"Succesfully created new view {viewname}")

        # commit and close
        con.commit()
        con.close()
    except Exception as e:
        logging.error(
            f"Could not drop / create MV for {bucket}, {agg}, {time_delta}: {e}")
        return None


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

    # converting refreshment time from str to float
    try:
        refreshment_time = float(mv_refreshment_time_str)
    except Exception as e:
        logging.error(e)
        raise e

    # refreshment of latest measurements
    schedule.every(5).minutes.do(refresh_latest_measurements_mv)

    # view updating for averages
    schedule.every(5).minutes.do(create_mv, bucket="10 min",
                                 agg="avg", time_delta=timedelta(days=1))
    schedule.every(30).minutes.do(create_mv, bucket="1 hour",
                                  agg="avg", time_delta=timedelta(weeks=1))
    schedule.every(2).hours.do(create_mv, bucket="4 hour",
                               agg="avg", time_delta=relativedelta(months=1))
    schedule.every(12).hours.do(create_mv, bucket="1 day",
                                agg="avg", time_delta=relativedelta(years=1))
    schedule.every(1).day.do(create_mv, bucket="1 week",
                             agg="avg", time_delta=relativedelta(years=30))

    # view updating for sums
    schedule.every(5).minutes.do(create_mv, bucket="10 min",
                                 agg="sum", time_delta=timedelta(days=1))
    schedule.every(30).minutes.do(create_mv, bucket="1 hour",
                                  agg="sum", time_delta=timedelta(weeks=1))
    schedule.every(2).hours.do(create_mv, bucket="4 hour",
                               agg="sum", time_delta=relativedelta(months=1))
    schedule.every(12).hours.do(create_mv, bucket="1 day",
                                agg="sum", time_delta=relativedelta(years=1))
    schedule.every(1).day.do(create_mv, bucket="1 week",
                             agg="sum", time_delta=relativedelta(years=30))

    logging.info("Trying to log ")
    # run all jobs once
    schedule.run_all()

    while True:
        schedule.run_pending()
