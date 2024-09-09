import logging
import os

import geopandas as gpd
import sqlalchemy


db_name = os.getenv("MOBILITY_DB_NAME")
db_host = os.getenv("MOBILITY_DB_SERVER")
db_port = os.getenv("MOBILITY_DB_PORT")
db_username = os.getenv("MOBILITY_DB_USERNAME")
db_password = os.getenv("MOBILITY_DB_PASSWORD")


def read_XDS_information(filepath: str) -> gpd.GeoDataFrame:
    """
    Reads INRIX data which connects INRIX XD-Segment IDs to actual geometries.
    """

    # check if path exists
    if not os.path.exists(filepath):
        msg = "Path to INRIX XDS data does not exist!"
        logging.error(msg)

        raise FileNotFoundError(filepath)

    # read in data
    try:
        inrix_xds_data = gpd.read_file(
            "./Germany_North_Rhine_Westphalia.geojson")
    except Exception as e:
        msg = f"Something went wrong reading INRIX XDS data from file: {e}"
        logging.error(msg)

        raise e

    return inrix_xds_data


def clean_XDS_information(inrix_xds_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Cleaning of INRIX data which connects INRIX XD-Segment IDs to actual geometries. 
    """

    try:

        # create latitude and longitude columns
        inrix_xds_data.loc[:, "longitude"] = \
            inrix_xds_data["geometry"].centroid.x
        inrix_xds_data.loc[:, "latitude"] = \
            inrix_xds_data.loc[:, "geometry"].centroid.y

        # set source to "INRIX"
        inrix_xds_data["source"] = "INRIX"

        # convert OpenStreetMap and XDSegment ID to int
        inrix_xds_data["OID"] = inrix_xds_data["OID"].astype(int)
        inrix_xds_data["XDSegID"] = inrix_xds_data["XDSegID"].astype(int)

        # set geometry column
        inrix_xds_data.set_geometry("geometry", inplace=True)

        # convert to EPSG:25832
        inrix_xds_data.set_crs(
            epsg=25832,
            allow_override=True,
            inplace=True)

    except Exception as e:
        msg = f"Something went wrong cleaning INRIX XDS data: {e}"
        logging.error(msg)
        raise e

    return inrix_xds_data


def write_to_database(cleaned_inrix_xds_data: gpd.GeoDataFrame) -> None:

    # establish database connection
    uri = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

    try:
        engine = sqlalchemy.create_engine(uri)
    except Exception as e:
        msg = f"There has been a problem connection to the database: {e}"
        logging.error(msg)

        raise e

    # insert into database
    try:
        cleaned_inrix_xds_data.to_postgis(
            name="inrix",
            con=engine,
            if_exists="replace",
            index=False)
    except Exception as e:
        msg = f"Something went wrong writing to database: {e}"
        logging.error(msg)

        raise e


if __name__ == "__main__":

    # logging
    logging.basicConfig(
        filename="inrix_xds_data.log",
        level=logging.WARNING,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S')

    # define filepath here
    filepath = "./Germany_North_Rhine_Westphalia.geojson"

    # read in data
    raw_inrix_xds_data = read_XDS_information(filepath)

    # clean data
    cleaned_inrix_xds_data = clean_XDS_information(raw_inrix_xds_data)

    # write cleaned data to databse
    write_to_database(cleaned_inrix_xds_data)
