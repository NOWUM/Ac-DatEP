import os
import logging

import sidebar
import coloring

from sqlalchemy import create_engine
import streamlit as st
import pandas as pd
from shapely import wkb

from dotenv import load_dotenv
load_dotenv()

# load authentication details
MOBILITY_DB_SERVER = os.getenv("MOBILITY_DB_SERVER")
MOBILITY_DB_PORT = os.getenv("MOBILITY_DB_PORT")
MOBILITY_DB_NAME = os.getenv("MOBILITY_DB_NAME")
MOBILITY_DB_USERNAME = os.getenv("MOBILITY_DB_USERNAME")
MOBILITY_DB_PASSWORD = os.getenv("MOBILITY_DB_PASSWORD")

DB_URI = f"postgresql://{MOBILITY_DB_USERNAME}:{MOBILITY_DB_PASSWORD}@{
    MOBILITY_DB_SERVER}:{MOBILITY_DB_PORT}/{MOBILITY_DB_NAME}"


def perform_default_page_jobs() -> None:
    """
    Displays sidebar, checks for datastreams and sensors in session state
    and queries those from database if needed.
    """

    # display the sidebar
    sidebar.display_sidebar()

    # check if API connection is available
    if not "engine" in st.session_state:
        create_sql_engine()

    # fecth datastreams
    if not "datastreams" in st.session_state:
        logging.info("Fetching datastreams from DB")
        sql = f"SELECT * FROM datastreams"
        st.session_state["datastreams"] = query_datasbase(sql)

    # fetch sensors
    if not "sensors" in st.session_state:
        logging.info("Fetching sensors from DB")
        sql = f"SELECT * FROM sensors"
        st.session_state["sensors"] = query_datasbase(sql)


def create_sql_engine() -> None:
    """
    Creates SQL Alchemy engine object for database access.
    """

    try:
        engine = create_engine(DB_URI)
    except Exception as e:
        logging.error(f"Could not create engine: {e}")
        display_error_message()
        st.stop()

    st.session_state["engine"] = engine


def display_not_auth_error() -> None:
    """
    Displays authentication error.
    """
    msg = f"Could not authenticate at API. Please make sure you have "
    msg += f"the correct credentials. "
    msg += f"If this problem keeps occuring please report it as bug."
    st.error(msg)


def display_error_message() -> None:
    """
    Displays general error.
    """
    msg = f"Something went wrong. "
    msg += f"If this problem keeps occuring please report it as bug."
    st.error(msg)


def display_no_data_warning() -> None:
    msg = f"No data to display!"
    st.warning(msg)


def query_datasbase(sql: str) -> pd.DataFrame:
    """
    Queries AC-DatEP DB and returns result as pd.DataFrame
    """
    try:

        engine = st.session_state["engine"]
        with engine.connect() as con:

            df = pd.read_sql(sql=sql, con=con)

        return df

    except Exception as e:
        logging.error(e)
        display_error_message()
        st.stop()


def prepare_location_dataframe():

    non_traffic_sensors = prepare_non_inrix_sensors()

    options = non_traffic_sensors["source"].unique().tolist()
    options.append("Alle")
    options.sort()
    source = st.selectbox(
        index=1,
        label="Quelle",
        options=options)

    if source != "Alle":
        sensors = filter_dataframe(
            non_traffic_sensors,
            filter_column="source",
            _filter=source)
    else:
        sensors = non_traffic_sensors

    return sensors

def prepare_non_inrix_sensors():

    non_traffic_sensors = st.session_state["sensors"].copy()

    # remove traffic sensors
    cond = non_traffic_sensors["source"] != 'INRIX'
    non_traffic_sensors = non_traffic_sensors[cond].reset_index(drop=True)

    # add color column for pydeck layer
    non_traffic_sensors["color"] = [[80, 180, 80]] * len(non_traffic_sensors)

    # round down lat lon columns for better readability
    non_traffic_sensors["latitude"] = non_traffic_sensors["latitude"].round(5)
    non_traffic_sensors["longitude"] = non_traffic_sensors["longitude"].round(5)

    return non_traffic_sensors


def get_latest_measurements(ds_ids: list[int]) -> pd.DataFrame:
    """
    Fetches latest measurements for given datastream_ids from
    'latest_measurements' materialized view.
    """

    sql = f"""
        SELECT *
        FROM latest_measurements
        WHERE datastream_id IN {tuple(ds_ids)}
    """

    df = query_datasbase(sql)

    return df


def query_materialized_view(
        viewname: str,
        ds_ids: list[str]) -> pd.DataFrame:

    sql = f"""
        SELECT *
        FROM {viewname}
        WHERE datastream_id IN {tuple(ds_ids)}
    """

    return query_datasbase(sql)


def get_timebucket_measurements(
        time_bucket: str,
        aggregation: str,
        ds_ids: list[int] | None = None,
        start_time: str | None = None) -> pd.DataFrame:
    """Fetches measurements in time bucket.

    Args:
        time_bucket (str): Time bucket to use. (e. g. '1 day', '1 month', '5 min', ...)
        aggregation (str): Aggregation type ('avg', 'sum') to use.
        ds_ids (list[int] | None, optional): List of datastream IDs to fetch measurements for. Defaults to None.
        start_time (str | None, optional): Earliest time to retrieve measurements for. Defaults to None.

    Returns:
        pd.DataFrame: Retrieved measurements
    """

    sql = f"""
        SELECT
            time_bucket('{time_bucket}', timestamp) AS bucket,
            {aggregation}(value) as value,
            datastream_id
        FROM measurements
    """

    if start_time:
        sql += f"""
            WHERE timestamp >= '{start_time}'
        """

    if ds_ids:
        if start_time:
            sql += "AND"
        else:
            sql += "WHERE"

        sql += f"""
             datastream_id IN {tuple(ds_ids)}
        """

    sql += f"""
        GROUP BY bucket, datastream_id
        ORDER BY bucket, datastream_id ASC
    """
    df = query_datasbase(sql)

    return df


def filter_dataframe(
        dataframe: pd.DataFrame,
        filter_column: str,
        _filter) -> pd.DataFrame:
    """
    Filters given dataframe on filter_column for _filter.
    """

    df = dataframe.copy()

    if isinstance(_filter, list):
        df = df[df[filter_column].isin(_filter)]
    else:
        df = df[df[filter_column] == _filter]

    df.reset_index(inplace=True, drop=True)

    return df


def build_measurements_query_strings(
        ds_ids: list[int] | int,
        skip_per_ds_id: int | None = None,
        limit_per_ds_id: int | None = None,
        order_by: str | None = None,
        order: str = "ascending") -> list[str]:
    """
    Builds a list of query strings for AC-DatEP database.

    Parameters:
    -------------
    ds_ids: list[int] | int
        List of datastream_ids or single datastream_id to build URLs for

    skip_per_ds_id: int | None, default None
        Number of values to skip per datastream_id

    limit_per_ds_id: int | None, default None
        Total number of values to retrieve per datastream_id

    order_by: str | None, default None
        Column to order values by

    order: str ("ASC" | "DESC"), default "ASC"
        Wether to order ascending or descending
    """

    query_strings = []

    if isinstance(ds_ids, int):
        ds_ids = [ds_id]

    for ds_id in ds_ids:
        query_str = f"SELECT * FROM measurements "
        query_str += f"WHERE datastream_id = {ds_id}"

        if order_by:
            query_str += f" ORDER BY {order_by} {order}"

        if skip_per_ds_id:
            query_str += f" OFFSET {skip_per_ds_id}"

        if limit_per_ds_id:
            query_str += f" LIMIT {limit_per_ds_id}"

        query_strings.append(query_str)

    return query_strings


def query_measurements(
        ds_ids: list[int] | int,
        skip_per_ds_id: int | None = None,
        limit_per_ds_id: int | None = None,
        order_by: str | None = None,
        order: str = "ascending") -> pd.DataFrame:
    """
    Queries measurements from AC-DatEP DB for given datastream_id(s).

    Parameters:
    -------------
    ds_ids: list[int] | int
        List of datastream_ids or single datastream_id to build URLs for

    skip_per_ds_id: int | None, default None
        Number of values to skip per datastream_id

    limit_per_ds_id: int | None, default None
        Total number of values to retrieve per datastream_id

    order_by: str | None, default None
        Column to order values by

    order: str ("ascending" | "descending"), default "ascending"
        Wether to order ascending or descending

    Returns:
    -------------
    measurements: pd.DataFrame
        Dataframe containing the measurements in long format
    """

    # build query strings to use for DB queries
    query_strings = build_measurements_query_strings(
        skip_per_ds_id=skip_per_ds_id,
        limit_per_ds_id=limit_per_ds_id,
        ds_ids=ds_ids,
        order_by=order_by,
        order=order)

    # send requests to URLs
    engine = st.session_state["engine"]
    dfs = []
    with engine.connect() as con:
        for query_str in query_strings:
            dfs.append(pd.read_sql(query_str, con))

    # concat DataFrames to make one large measurements dataframe
    try:
        measurements = pd.concat(objs=dfs, copy=False, ignore_index=True)
    except ValueError as e:
        st.error(f"Measurements could not be fetched...")
        return None

    return measurements


def add_sensors_datastreams_to_measurements(
        measurements: pd.DataFrame,
        datastreams: pd.DataFrame,
        sensors: pd.DataFrame) -> pd.DataFrame:
    """
    Addes given datastreams and sensors data to measurements data,
    drops duplicated columns
    """

    try:
        # remove confidentiality columns if they exist
        if "confidential" in measurements.columns:
            measurements.drop(columns="confidential", inplace=True)

        if "confidential" in datastreams.columns:
            datastreams.drop(columns="confidential", inplace=True)

        if "confidential" in sensors.columns:
            sensors.drop(columns="confidential", inplace=True)

        # merge datastreams onto sensors
        data = pd.merge(
            left=measurements,
            right=datastreams,
            left_on="datastream_id",
            right_on="id",
            how="left")

        # remove "id" column as this is same as "datastream_id"
        data.drop(
            columns="id",
            inplace=True)

        # merge sensor data onto measurements
        data = pd.merge(
            left=data,
            right=sensors,
            left_on="sensor_id",
            right_on="id",
            how="left")

        # rename ex_id columns
        data.rename(
            columns={
                "ex_id_x": "datastream_ex_id",
                "ex_id_y": "sensor_ex_id"},
            inplace=True)

        # again drop ID column
        data.drop(
            columns="id",
            inplace=True)

        return data

    except Exception as e:
        logging.error(e)
        display_error_message()
        st.stop()


def add_color_to_data(
        data: pd.DataFrame,
        min_value: float,
        max_value: float,
        value_col: str = "value",
        colorscale: str = "Plotly3",
        color_dict: dict | None = None) -> pd.DataFrame:
    """
    Adds color column to given data.

    Parameters:
    -------------
    data: pd.DataFrame
        The data to add color to.

    min_value: float
        Minimum value to use for colorscale.

    max_value: float
        Maximum value to use for colorscale.

    value_col: str = "value"
        Value column to use for scaling.

    colorscale: str = "Plotly3"
        Plotly colorscale to use.

    color_dict: dict[str, list[int, int, int]]
        Dictionary to containing pairs of value: [r, g, b]
        to use as colorscale. Will overwrite "colorscale"
        parameter.
    """

    if not color_dict:
        color_dict = coloring.get_color_dict(
        min_value=min_value,
        max_value=max_value,
        colorscale=colorscale)

    data["adjusted_value"] = data[value_col].copy()
    data.loc[data["adjusted_value"] > max_value, "adjusted_value"] = max_value
    data.loc[data["adjusted_value"] < min_value, "adjusted_value"] = min_value

    data["color"] = data["adjusted_value"].map(color_dict)

    data.drop(columns="adjusted_value", inplace=True)

    return data


def convert_shapely(shapely_obj) -> list[list[float]]:
    """
    Converts shapely objects to coordinate lists.

    Parameters:
    -------------
    shapely_obj:
        shapely object to convert

    Returns:
    -------------
    coords: list[list[float]]
        List of x- and y-coordinates as list
    """

    coords = shapely_obj.coords.xy

    x_coords = list(coords[0])
    y_coords = list(coords[1])

    coords = list(zip(x_coords, y_coords))

    coords = [list(coord) for coord in coords]

    return coords


def convert_to_timezone(
        data: pd.DataFrame,
        timezone: str = "Europe/Berlin") -> pd.Series:
    """Converts pd.Series with timestamps to timezone.

    Args:
        data (pd.Series): The DataFrame to convert columns from.
        timezone (str, optional): Timezone to convert to. Defaults to "Europe/Berlin".

    Returns:
        pd.Series: Converted series.
    """
    data = data.copy()

    format = "Datum: %d.%m.%Y\n Uhrzeit: %X"
    data["timestamp_utc"]  = data["timestamp"].dt.tz_localize("UTC")
    data["timestamp_local"] = data["timestamp_utc"].dt.tz_convert(timezone)
    data["timestamp_local_str"] = data["timestamp_local"].dt.strftime(format)

    return data

def fetch_prepare_measurements(
        datastreams: pd.DataFrame,
        sensors: pd.DataFrame,
        viewname: str | None = None,
        time_bucket: str | None = None,
        aggregation: str | None = None,
        start_time: str | None = None,
        is_traffic: bool = False) -> pd.DataFrame:
    """
    Fetches and prepares measurements

    Args:
        datastreams (pd.DataFrame): Datastreams to fetch measurements for
        sensors (pd.DataFrame): Sensors belonging to given datastreams
        viewname (str | None, optional): viewname to query for measurements. Defaults to None.
        time_bucket (str | None, optional): Time bucket to use. Fetches latest if not provided. Defaults to None.
        aggregation (str | None, optional): Aggregation method for bucket to use. Fetches latest if not provided. Defaults to None.
        start_time (str | None, optional): Time, to fetch earliest measurements from. Defaults to None.
        is_traffic (bool, optional): Wether datastreams are of motor vehicle traffic. Defaults to False.

    Returns:
        pd.DataFrame: Prepared measurements with datastreams and sensors info.
    """

    # query last measurement for each datastream
    ds_ids = datastreams["id"].tolist()


    if not time_bucket and not viewname:
        # query last measurement for each ds_id
        logging.info("Fetching latest measurements")
        measurements = get_latest_measurements(ds_ids=ds_ids)
    elif viewname:
        logging.info("Fetching materialized view")
        measurements = query_materialized_view(
            viewname=viewname,
            ds_ids=ds_ids)

        # bucketed data will have name column "bucket",
        # switch that to "timestamp"
        measurements.rename(columns={"bucket": "timestamp"}, inplace=True)

    else:
        logging.info("Fetching timebucket measurements")
        measurements = get_timebucket_measurements(
            ds_ids=ds_ids,
            time_bucket=time_bucket,
            aggregation=aggregation,
            start_time=start_time)

        # bucketed data will have name column "bucket",
        # switch that to "timestamp"
        measurements.rename(columns={"bucket": "timestamp"}, inplace=True)

    if measurements.empty:
        return measurements

    # convert timestamps to Europe/Berlin timezone
    measurements = convert_to_timezone(measurements)

    # add datastreams and sensor data to measurements
    logging.info("Adding sensors datastreams")
    data = add_sensors_datastreams_to_measurements(
        measurements=measurements,
        datastreams=datastreams,
        sensors=sensors)

    if is_traffic:
        data["geometry"] = wkb.loads(data["geometry"])
        data["geometry"] = data["geometry"].apply(convert_shapely)

    logging.info("Done preparing map data")

    return data


def calc_charging_stations_percentages(gdf):
    """
    Helper function to calculate free / occupied
    station values / percentages / ... per group
    """

    num_stations = len(gdf)
    gdf["num_stations"] = num_stations

    free_stations = len(gdf[gdf["value"] == 0])
    gdf["free_stations"] = free_stations

    used_stations = len(gdf[gdf["value"] == 1])
    gdf["used_stations"] = used_stations

    out_of_order_stations = len(gdf[gdf["value"] == -1])
    gdf["ooo_staions"] = out_of_order_stations

    gdf["perct_free"] = gdf["free_stations"] / gdf["num_stations"]
    gdf["perct_free"] = gdf["perct_free"].round(2)

    gdf["perct_used"] = 1 - gdf["perct_free"]
    gdf["perct_used"] = gdf["perct_used"].round(2)

    return gdf


def add_charging_stations_info(data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds power info and info over free / occupied stations to given data
    """

    # query stations data (power)
    sql = """
        select
            type AS power_type,
            max_power,
            sensor_id
        from chargingstations
    """
    stations_data = query_datasbase(sql)

    data = pd.merge(
        left=data,
        right=stations_data,
        on="sensor_id")

    data = data.groupby("sensor_id", as_index=False).apply(calc_charging_stations_percentages)

    return data


def add_trafficlanes_info(data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds trafficlane info to given data

    Args:
        data (pd.DataFrame): bike traffic data

    Returns:
        pd.DataFrame: bike traffic data with trafficlanes info
    """

    sql = """
        SELECT *
        FROM trafficlanes
    """
    trafficlanes = query_datasbase(sql)

    return pd.merge(
        left=data,
        right=trafficlanes,
        on="datastream_id",
        how="left")


def get_viewname_from_user_input(
        label: str,
        agg_type: str) -> str:

    choice = st.radio(
        label=label,
        label_visibility="collapsed",
        options=["1T", "1W", "1M", "1J", "MAX"],
        horizontal=True)

    base_view_name = "bucketed_measurements_"

    if choice == "1T":
        view_name = base_view_name + "10min_" + agg_type
    elif choice == "1W":
        view_name = base_view_name + "1hour_" + agg_type
    elif choice == "1M":
        view_name = base_view_name + "4hour_" + agg_type
    elif choice == "1J":
        view_name = base_view_name + "1day_" + agg_type
    elif choice == "MAX":
        view_name = base_view_name + "1week_" + agg_type

    return view_name
