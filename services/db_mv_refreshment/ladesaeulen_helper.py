import os
import logging

import pandas as pd
import holidays
import numpy as np

from dotenv import load_dotenv
load_dotenv()

DB_URI = os.getenv("DB_URI")


def get_datastreams():

    logging.info("Fetching datastreams for ladesaeulen...")

    sql = f"SELECT * FROM datastreams WHERE type = 'E-Ladepunkt'"

    return pd.read_sql(sql, DB_URI)


def get_sensors():

    sql = f"""
        SELECT * FROM sensors
        WHERE id IN (
            SELECT sensor_id FROM datastreams
            WHERE type = 'E-Ladepunkt')"""

    return pd.read_sql(sql, DB_URI)


def get_ladesaeulen_info():

    sql = f"SELECT * FROM chargingstations"

    return pd.read_sql(sql, DB_URI)


def get_hourly(id):

    try:
        df = get_measurements(datastream_id=id)
        df = resample_to_minutely(df)
        df = add_time_infos(df)

        df = calculate_hourly_usage(df)

        df = df[["timestamp", "datastream_id", "hourly_usage_percent"]]

        return df

    except Exception as e:
        return pd.DataFrame()


def get_measurements(
        time_bucket: str | None = None,
        datastream_id: list[int] | int | None = None,
        remove_missing: bool = True):

    if time_bucket:
        select_statement = f"""
            time_bucket('{time_bucket}', timestamp) as timestamp,
            avg(value) as value,
            datastream_id
        """
        group_statement = "GROUP BY timestamp, datastream_id"

    else:
        select_statement = "*"
        group_statement = ""

    if remove_missing:
        where_statement = "AND value != -1"
    else:
        where_statement = ""

    if not datastream_id:
        sql = f"""
            SELECT
                {select_statement}
            FROM measurements
            WHERE datastream_id IN (
                SELECT id
                FROM datastreams
                WHERE type = 'E-Ladepunkt')
            {where_statement}
            {group_statement}
            ORDER BY timestamp, datastream_id ASC
        """

    elif isinstance(datastream_id, (np.int64, int)):
        sql = f"""
            SELECT {select_statement}
            FROM measurements
            WHERE datastream_id = {datastream_id}
            {where_statement}
            {group_statement}
            ORDER BY timestamp ASC
        """

    elif isinstance(datastream_id, list):
        sql = f"""
            SELECT {select_statement}
            FROM measurements
            WHERE datastream_id IN {tuple(datastream_id)}
            {where_statement}
            {group_statement}
            ORDER BY timestamp, datastream_id ASC
        """

    return pd.read_sql(sql, DB_URI)


def add_time_infos(
        df: pd.DataFrame,
        time_col: str = "timestamp"):

    df = df.copy()

    df["hour"] = df[time_col].dt.hour
    df["month"] = df[time_col].dt.month

    df["dayofweek"] = df[time_col].dt.dayofweek
    df["dayofmonth"] = df[time_col].dt.day
    df["dayofyear"] = df[time_col].dt.dayofyear

    df["date"] = df[time_col].dt.date

    df["is_weekend"] = df["dayofweek"].isin([5, 6])

    years = list(range(2022, 2024))
    hols = holidays.CountryHoliday(
        country="Germany",
        subdiv="NW",
        years=years)
    df["is_holiday"] = df["date"].isin(hols)

    df["is_workday"] = ~(df["is_weekend"] | df["is_holiday"])
    df["is_working_hours"] = (df["hour"].isin(list(range(7, 17))) & df["is_workday"])

    return df


def resample_to_minutely(
        df: pd.DataFrame,
        time_col: str = "timestamp"):

    df = df.copy()

    df.set_index(time_col, inplace=True)

    df = df.resample("1min").ffill(limit=15)

    df.dropna(subset="value", inplace=True)

    return df.reset_index()


def calculate_hourly_usage(df: pd.DataFrame):

    def calc(gdf):

        gdf["hourly_usage_percent"] = gdf["value"].sum() / 60

        return gdf

    df = df.copy()

    df = df.groupby(["hour", "date"], as_index=False).apply(calc)

    df.reset_index(inplace=True, drop=True)

    df = df.resample("1h", on="timestamp").mean(numeric_only=True)

    return df.reset_index().sort_values("timestamp", ignore_index=True)


def calculate_daily_usage(df: pd.DataFrame):

    def calc(gdf):

        gdf["daily_usage_percent"] = gdf["value"].sum() / (60 * 24)

        return gdf

    df = df.copy()

    df = df.groupby("date", as_index=False).apply(calc)

    df.reset_index(inplace=True, drop=True)

    df = df.resample("1d", on="timestamp").mean(numeric_only=True)

    return df.reset_index().sort_values("timestamp", ignore_index=True)


def merge_infos_to_measurements(
        measurements: pd.DataFrame,
        datastreams: pd.DataFrame,
        info: pd.DataFrame):

    df = pd.merge(
        left=measurements,
        right=datastreams[["id", "sensor_id"]],
        left_on="datastream_id",
        right_on="id",
        how="left")

    df = pd.merge(
        left=df,
        right=info,
        on="sensor_id",
        how="left")

    return df
