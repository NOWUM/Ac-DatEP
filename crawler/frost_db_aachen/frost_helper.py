from shapely.geometry import Point, Polygon, LineString
import logging
import os
import pandas as pd
import geopandas as gpd
import sqlalchemy
from datetime import timedelta
from sqlalchemy.exc import IntegrityError

from typing import List

from dotenv import load_dotenv
load_dotenv()

WEATHERTYPES = [
    'SIGNIFICANTWEATHER',
    'WINDDIRECTION',
    'HUMIDITY', 
    'TEMPERATURE',
    'DEWPOINT',
    'WINDSPEED',
    'PROBABILITYOFPRECIPITATION']

MOBILITY_DB_NAME = os.getenv("MOBILITY_DB_NAME")
MOBILITY_DB_SERVER = os.getenv("MOBILITY_DB_SERVER")
MOBILITY_DB_PORT = os.getenv("MOBILITY_DB_PORT")
MOBILITY_DB_USERNAME = os.getenv("MOBILITY_DB_USERNAME")
MOBILITY_DB_PASSWORD = os.getenv("MOBILITY_DB_PASSWORD")

mobility_url = f"postgresql://{MOBILITY_DB_USERNAME}:{MOBILITY_DB_PASSWORD}@{MOBILITY_DB_SERVER}:{MOBILITY_DB_PORT}/{MOBILITY_DB_NAME}"

def fetch_things_charger(thing: dict) -> dict:
    """
    Transforms imported frost data into internal timescaledb format.

    Parameters
    ----------
    thing : dict
        One FROST thing's value field.

    Returns
    -------
    thing_chargingstation : dict
        One line of the later sensors table still using the external ID.

    """
    thing_id = thing['@iot.id']
    thingprops = thing['properties'].get('props')
    if thingprops:
        lstat_type = thingprops.get('chargePointType',
                                    'none')
        max_power = thingprops.get('ratings', {}).get('maximumPower', 0)
    else:
        lstat_type = 'AC'
        max_power = 0

    thing_chargingstation = {'thing_id': thing_id,
                             'type': lstat_type,
                             'max_power': max_power}
    return thing_chargingstation


def fetch_things_parking(thing: dict, encryption: str) -> dict:
    """
    Transforms imported frost data into internal timescaledb format.

    Parameters
    ----------
    thing : dict
        One FROST thing's value field.
    encryption : str
        The category of thing.

    Returns
    -------
    thing_parking : dict
        One line of the later sensors table still using the external ID.

    """
    thing_id = thing['@iot.id']
    parking_type = thing['properties'].get('species', 'none')
    if encryption == 'parkhaus':
        parking_capacity = thing['properties'].get(
            'props', {}).get('capacity', 0)
    elif encryption == 'location':
        parking_capacity = len(thing['properties'].get(
            'props', {}).get('subLocationIds', [0]))
    elif encryption == 'fläche':
        if thing['properties'].get('active'):
            parking_capacity = 2
        else:
            parking_capacity = 0

    thing_parking = {'thing_id': thing_id,
                     'type': parking_type,
                     'capacity': parking_capacity}

    return thing_parking


def fetch_things_traffic(ds: int, thing: dict) -> dict:
    """
    Creates an entry in the traffic lane table from a FROST thing.

    Parameters
    ----------
    ds: int
        Datastream ID relating to the thing.
    
    thing : dict
        One FROST thing relating to traffic measurement.

    Returns
    -------
    thing_traffic : dict
        One line in the traffic lane table.

    """
    ds_id = ds
    lane_speedlimit = thing['properties'].get('props', {}).get('speedLimit', 0)

    thing_traffic = {'datastream_id': ds_id,
                     'lane_speedlimit': lane_speedlimit}

    return thing_traffic


def fetch_datastreams_metadata(datastream_id: int,
                               description: str,
                               klasse: str) -> dict:
    """
    Transforms a FROST datastream into a line in the datastreams table.

    Parameters
    ----------
    datastream_id : int
        External datastream ID.
    description : str
        Description of the datastream.
    klasse : str
        Category of the datastream.

    Returns
    -------
    datastream_metadata : dict
        One line in the datastream table still using the external ID.

    """
    datastream_metadata = {'ds_id': datastream_id,
                           'description': description,
                           'klasse': klasse}
    return datastream_metadata


def get_klasse_from_datastream(ds_value: dict) -> str:
    """
    Tries to retrieve "klasse" from datastream.

    Parameters:
    ---------------
        ds_value: dict
            the datastream to get the klasse from

    Returns:
    ---------------
        klasse: str
            The inferred klasse
    """

    # try to fetch from key "klasse"
    klasse = ds_value['properties'].get('Klasse')

    # if there is no key "klasse" use "type"
    if not klasse:
        klasse = ds_value['properties'].get('type')

    # if there is no key "type" check for weather, otherwise
    # set "klasse" to "unknown"
    if not klasse:
        if ds_value['description'] in WEATHERTYPES:
            klasse = 'Wetter'
        else:
            klasse = 'unknown'

    return klasse


def get_coordinates_from_datastream(ds_value: dict):

    observed_area = ds_value.get('observedArea')
    if not observed_area:

        # try to find chargepointlocation if exists
        try:
            longitude = ds_value['chargePointLocation']['coordinates'].get('lon', 0)
            latitude = ds_value['chargePointLocation']['coordinates'].get('lat', 0)
            area = [longitude, latitude]

            geometries = fetch_thing_coordinates(
                coordinate_type='Point',
                area=area,
                ds_id=ds_value['@iot.id'])

            return geometries

        except KeyError:
            logging.info('No viable area for datastream %s!' %
                    (ds_value['@iot.id']))
            geometries = fetch_thing_coordinates(
                coordinate_type='unknown',
                area=None,
                ds_id=ds_value['@iot.id'])

            return geometries

    else:
        observed_type = observed_area.get('type')
        area = observed_area.get('coordinates')
        geometries = fetch_thing_coordinates(
            coordinate_type=observed_type,
            area=area,
            ds_id=ds_value["@iot.id"])

        return geometries


def create_trafficlanes_entry(datastream: dict) -> dict:
    """
    Creates an entry for the traffic lane table from a FROST datastream

    Parameters
    ----------
    datastream : dict
        One FROST datastream.

    Returns
    -------
    datastream_trafficlanes : dict
        One line in the traffic lane table.

    """
    lane = datastream['properties']['Fahrspur']
    lane_ID = datastream['properties']['FahrspurID']
    lane_ex_ID = datastream['@iot.id']
    aggregation = datastream['properties']['Aggregation']

    datastream_trafficlanes = {'lane': lane,
                               'lane_ID': lane_ID,
                               'lane_ds_ID': lane_ex_ID,
                               'aggregation': aggregation}

    return datastream_trafficlanes


def fetch_thing_coordinates(
        ds_id: int,
        coordinate_type: str = "unkown",
        area: List | None = None) -> dict:
    """
    Transforms coordinates of a FROST thing into the data format of the
    timescaledb Sensors table. For Polygons, Latitude and Longitude of the
    centroid are used in addition to the full coordinates.

    Parameters
    ----------
    ds_id : int
        FROST ID of the corresponding datastream.
    coordinate_type : str = None
        "Point", "Polygon" or "LineString.
    area : List | None = None
        Full coordinates in FROST data format. Optional.

    Returns
    -------
    coordinates: dict
        Dictionary including a geometry object as well as longitude and latitude.

    """
    if coordinate_type == 'Point':
        coordinates = {'coordinate_type': coordinate_type,
                       'longitude': area[0],
                       'latitude': area[1],
                       'geometry': Point(area),
                       'ds_id': ds_id}
    elif coordinate_type == 'Polygon':
        polylist = []
        first_area = area[0]
        for coordinate_pair in first_area:
            polylist.append((coordinate_pair[0], coordinate_pair[1]))
        polygon = Polygon(polylist)
        coordinates = {'coordinate_type': coordinate_type,
                       'longitude': polygon.centroid.x,
                       'latitude': polygon.centroid.y,
                       'geometry': polygon,
                       'ds_id': ds_id}
    elif coordinate_type == 'LineString':
        linestring = LineString(area)
        coordinates = {'coordinate_type': coordinate_type,
                       'longitude': linestring.centroid.x,
                       'latitude': linestring.centroid.y,
                       'geometry': linestring,
                       'ds_id': ds_id}
    else:
        coordinates = {'coordinate_type': 'unknown',
                       'longitude': '',
                       'latitude': '',
                       'geometry': None,
                       'ds_id': ds_id}
    return coordinates


def external_to_internal(table_name: str, ex_id: int) -> int:
    """
    Translates external FROST IDs of either datastreams or sensors/things to
    internal ones.

    Parameters
    ----------
    table_name : str
        datastreams or sensors.
    ex_id : int
        FROST ID corresponding to things or datastreams.

    Returns
    -------
    inernal_id : int
        Internal ID corresponding to the input.

    """
    sql_connect = mobility_url
    engine = sqlalchemy.create_engine(sql_connect)
    with engine.connect() as conn:
        sql = f"""
            SELECT {table_name}.id
            FROM datastreams RIGHT OUTER JOIN sensors
            ON datastreams.sensor_id = sensors.id
            WHERE {table_name}.ex_id = \'{ex_id}\'
            AND sensors.source = \'Frost\'
        """
        internal_id = pd.read_sql(sql, conn)

    return internal_id['id'][0]


def lookup_id_dict(
        table_name: str,
        external_ids: list[int]) -> pd.DataFrame:
    """
    Creates a table with all internal IDs and their corresponding external IDs.

    Parameters
    ----------
    table_name : str
        datastreams or sensors.
    external_ids : list[int]
        List of external ids.

    Returns
    -------
    internal_ids : pd.DataFrame
        table of corresponding internal and external ids.

    """

    try:
        engine = sqlalchemy.create_engine(mobility_url)
        with engine.connect() as conn:
            internal_ids = pd.read_sql(
                sql=f'''
                    SELECT {table_name}.id,  {table_name}.ex_id
                    FROM datastreams RIGHT OUTER JOIN sensors 
                    ON datastreams.sensor_id = sensors.id
                    WHERE sensors.source = \'Frost\'''',
                con=conn)
            
        internal_ids.ex_id = pd.to_numeric(internal_ids.ex_id)
        
        internal_ids = internal_ids[internal_ids["ex_id"].isin(external_ids)]

        return internal_ids

    except Exception as e:
        logging.error(f"Something went wrong looking up ID dict: {e}")
        return None


def lookup_max(table_name: str, column_name: str) -> float:
    """
    Finds out the maximum value in a given column of a given table.

    Parameters
    ----------
    table_name : str
        One timescaledb table.
    column_name : str
        One column in the table.

    Returns
    -------
    max_value: float
        The maximum value.

    """
    sql_connect = mobility_url
    engine = sqlalchemy.create_engine(sql_connect)
    with engine.connect() as conn:
        max_value = pd.read_sql(f'SELECT {column_name} '
                                f'FROM {table_name} '
                                f'ORDER BY {column_name} desc '
                                f'FETCH FIRST 1 ROWS ONLY', conn)
    return max_value[column_name][0]


def make_sensor_table(things: pd.DataFrame,
                      descriptions: list,
                      geometry: pd.Series,
                      confidential: pd.Series) -> tuple:
    """
    Combines fragmented data into one coherent sensor table.

    Parameters
    ----------
    things : pd.DataFrame
        Table of frost Things.
    descriptions : list
        Table of descriptions compiled from things and datastreams.
    geometry : pd.DataFrame
        Table of geometries compiled from things and datastreams.

    Returns
    -------
    num_duplicates : int
        Number of duplicates in the table. Useful only for debugging purposes.

    sensors : gpd.GeoDataFrame
        table of unique sensors

    """
    length = len(things)
    sensors = {'source': ['Frost'] * length,
               'ex_id': things['thing_id'].to_list(),
               'description': descriptions.to_list(),
               'longitude': geometry['longitude'].to_list(),
               'latitude': geometry['latitude'].to_list(),
               'confidential': confidential.to_list()}
    sensors = pd.DataFrame(sensors, index=geometry.index)
    geometry_series = gpd.GeoSeries(
        geometry['geometry'], index=geometry.index, crs='EPSG:25832')
    sensors = gpd.GeoDataFrame(sensors, geometry=geometry_series)

    size_before = len(sensors)
    sensors = sensors[~sensors.duplicated('ex_id')]
    num_duplicate = size_before - len(sensors)

    return num_duplicate, sensors


def make_ds_table(thing_id_dict: pd.DataFrame,
                  things: pd.DataFrame,
                  datastreams: pd.DataFrame,
                  confidential: pd.Series) -> tuple:
    """
    Combines fragmented data into one coherent datastreams table.

    Parameters
    ----------
    thing_id_dict : pd.DataFrame
        Table relating external thing IDs with internal Sensor IDs.
    things : pd.DataFrame
        Datastream with FROST thing data relevant to the datastream table.
    datastreams : pd.DataFrame
        Datastream with FROST datastream data relevant to the datastream table.

    Returns
    -------
    num_duplicate : int
        The number of duplicate datastreams that were removed from the table.
    datastreams : pd.DataFrame
        Datastream according to the datastream table in timescaleDB.

    """

    things = things.set_index("thing_id").join(
        other=thing_id_dict.set_index("ex_id"), how="inner")
    things = things.drop_duplicates()

    things = things[things["ds_id"].isin(datastreams["ds_id"])]

    sensor_ids = []
    ex_ids = []
    units = []
    types = []
    confidentials = []

    for index, datastream in datastreams.iterrows():
        match datastream["klasse"]:
            case "E-Ladepunkt":
                sensor_ids
                ex_ids
                units.append("Occupancy status")
                types.append(datastream["klasse"])
            case "Parkobjekt" | "ParkingArea" | "ParkingLocation":
                units.append("Vacant Spaces")
                types.append(datastream["klasse"])
            case "cC1" | "cC2" | "cC3" | "vC1" | "vC2" | "vC3":
                units.append("Vehicles Counted")
                types.append("motor traffic measurement")
            case "Bike":
                units.append("Bikes counted")
                types.append("bike traffic measurement")
            case _:
                units.append("unknown")
                types.append("unknown")

    datastreams = {'sensor_id': things["id"].to_list(),
                   'ex_id': things["ds_id"].to_list(),
                   'type': types,
                   'unit': units,
                   'confidential': confidential}
    datastreams = pd.DataFrame(datastreams)
    size_before = len(datastreams)
    datastreams = datastreams.drop_duplicates()
    num_duplicate = len(datastreams) - size_before

    return num_duplicate, datastreams


def make_traffic_table(traffic_things: pd.DataFrame,
                       traffic_datastreams: pd.DataFrame) -> pd.DataFrame:
    """
    Combines fragmented data into one coherent trafficlane table.

    """

    ds_trafficlanes = pd.DataFrame(traffic_datastreams)

    traffic_id_dict = lookup_id_dict(table_name='datastreams',
                                     external_ids=ds_trafficlanes['lane_ds_ID'].to_list())

    ds_trafficlanes = pd.merge(left = ds_trafficlanes,
                               right = traffic_id_dict,
                               how = "inner",
                               left_on = "lane_ds_ID",
                               right_on = "ex_id")

    trafficlanes = {'datastream_id': ds_trafficlanes['id'].to_list(),
                    'lane': ds_trafficlanes['lane'].to_list(),
                    'speedlimit': traffic_things['lane_speedlimit'].to_list(),
                    'aggregation': ds_trafficlanes['aggregation'].to_list()}
    trafficlanes = pd.DataFrame(trafficlanes)
    return trafficlanes


def make_chargingstations_table(sensor_id_dict: pd.DataFrame,
                                rows_chargingstations: list,
                                use_sensors: list) -> pd.DataFrame:
    """
    Combines fragmented data into one coherent chargingstations table.

    """
    chargingstations_table = pd.DataFrame(rows_chargingstations,
                                          columns=["thing_id",
                                                   "type",
                                                   "max_power"])
    chargingstations_table = chargingstations_table.drop_duplicates()

    chargingstations_table = chargingstations_table.loc[chargingstations_table["thing_id"].isin(use_sensors)]
    chargingstations_id_dict = lookup_id_dict(table_name='Sensors',
                                              external_ids=chargingstations_table['thing_id'].to_list())

    chargingstations_table = pd.merge(left = chargingstations_table,
                                      right = chargingstations_id_dict,
                                      how = "inner",
                                      left_on = "thing_id",
                                      right_on = "ex_id")

    chargingstations_table.rename(columns={"id": "sensor_id"}, inplace = True)
    chargingstations_table = chargingstations_table[["thing_id", "type", "max_power", "sensor_id"]]
    return chargingstations_table


def make_parking_table(rows_parking: list, use_sensors: set) -> pd.DataFrame:
    """
    Combines fragmented data into one coherent parking table.

    """
    parking_table = pd.DataFrame(rows_parking,
                                 columns=["thing_id",
                                          "type",
                                          "capacity"])
    parking_table = parking_table.loc[parking_table["thing_id"].isin(use_sensors)]
    parking_table = parking_table.drop_duplicates()
    parking_id_dict = lookup_id_dict(table_name='Sensors',
                                     external_ids=parking_table['thing_id'].to_list())

    parking_table = pd.merge(left = parking_table,
                             right = parking_id_dict,
                             how = "inner",
                             left_on = "thing_id",
                             right_on = "ex_id")

    parking_table.rename(columns={"id": "sensor_id"}, inplace = True)
    parking_table = parking_table[["thing_id", "type", "capacity", "sensor_id"]]
    return parking_table

# check default start date param
def get_starttime(ds: int, DEFAULT_START_DATE: str) -> str:
    """Fetches the date one specific datastream was last updated

    For this purpose, the date of the most recent observation corresponding
    to that datastream is requested from the database.        

    Parameters
    ----------
    ds : int
        Datastream_ID for the relevant datastream.

    Returns
    -------
    DEFAULT_START_DATE : str
        start date in accordance to FROST date format

    """

    try:
        sql_connect = mobility_url
        engine = sqlalchemy.create_engine(sql_connect)

        internal_id = external_to_internal('datastreams', ds)
        sql = f'select timestamp from measurements where datastream_id = {internal_id} order by timestamp desc limit 1'
        with engine.connect() as conn:
            latest = pd.read_sql(sql, conn, parse_dates=[
                                 'timestamp']).values[0][0]
        latest = pd.to_datetime(latest, unit='ns')
        latest = latest + timedelta(seconds=10)
        timestring = str(latest).split(' ', 1)
        time4frost = timestring[0] + 'T' + \
            timestring[1].split('+', 1)[0] + 'Z'
        return time4frost
    except Exception:
        logging.info(
            f'No viable start date for datastream {ds} found. Using default start date.')
        return DEFAULT_START_DATE


def feed_table_pd(
        tablename: str,
        data: pd.DataFrame):
    """
    Feeds a pandas dataframe to the database

    Parameters
    ----------
    tablename : str
        The table the dataframe is supposed to be written to.
    data : pd.DataFrame
        Dataframe corresponding to the given table.

    Returns
    -------
    None.

    """

    if len(data) < 1:
        logging.info("No data to write into DB")
        return True

    logging.info(f"Trying to write {len(data)} entries into database!")

    try:
        sql_connect = mobility_url
        engine = sqlalchemy.create_engine(sql_connect)

        with engine.connect() as conn:

            # try to write as chunk
            # sometimes there are duplicates which will void the whole transaction
            # thus we except the error and try to write line by line into the DB
            try:
                data.to_sql(
                    name=tablename,
                    con=conn,
                    if_exists="append",
                    index=False)
                logging.info("Succesfully wrote data into DB")
                return True

            except IntegrityError as e:
                msg = f"Could not write data of length {len(data)} into DB, "
                msg += f"trying to write entry by entry..."
                logging.info(msg)

            # write line by line
            # this takes a while...
            for idx, row in data.iterrows():
                try:
                    tmp_df = pd.DataFrame(row).T
                    tmp_df.to_sql(
                        name=tablename,
                        con=conn,
                        if_exists="append",
                        index=False)
                except IntegrityError as e:
                    pass

        logging.info("Succesfully wrote data into database!)")

    except Exception as e:
        logging.error(f"Could not connect to database: {e}")



def feed_table_gpd(tablename: str,
                   data: gpd.GeoDataFrame):
    """
    Feeds a geopandas geodataframe to the database

    Parameters
    ----------
    tablename : str
        The table the dataframe is supposed to be written to.
    data : gpd.GeoDataFrame
        Dataframe corresponding to the given table.

    Returns
    -------
    None.

    """

    try:
        sql_connect = mobility_url
        engine = sqlalchemy.create_engine(sql_connect)
        with engine.connect() as conn:
            data.to_postgis(name=tablename,
                            con=conn,
                            if_exists='append',
                            index=False)
    except Exception as e:
        logging.error(f"Could not connect to database: {e}")


def is_confidential(
        id: int,
        table_name: str) -> bool:
    """
    Looks up whether a datastream is confidential 
    """

    try:
        sql_connect = mobility_url
        engine = sqlalchemy.create_engine(sql_connect)
        with engine.connect() as conn:
            confidential = pd.read_sql(f'SELECT confidential '
                                    f'FROM {table_name} '
                                    f'WHERE {table_name}.id = {id}', conn)
        return confidential.loc[0, "confidential"]

    except Exception as e:
        msg = f"Something went wrong looking up confidential value for datastream ID {id}: {e}"
        logging.warning(msg)
        return True


def query_measurements(
        con,
        source=None,
        limit=None,
        order=None):

    if source:
        where_clause = f""" WHERE sensors.source = '{source}'"""
    else:
        where_clause = ""

    if order:
        order_clause = f""" ORDER BY measurements.timestamp {order}"""
    else:
        order_clause = ""

    if limit:
        limit_clause = f""" LIMIT {limit}"""
    else:
        limit_clause = ""

    sql = f"""
        SELECT
            datastream_id,
            type,
            unit,
            timestamp,
            value,
            sensor_id,
            measurements.confidential
        FROM
            measurements
        LEFT JOIN
            datastreams
        ON
            datastreams.id = measurements.datastream_id
        LEFT JOIN
            sensors
        ON
            sensors.id = datastreams.sensor_id
    """
    sql += where_clause
    sql += order_clause
    sql += limit_clause

    return pd.read_sql(sql, con)