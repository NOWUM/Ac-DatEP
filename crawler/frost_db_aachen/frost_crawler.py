import pandas as pd
import requests
import logging
import os
import time
from sqlalchemy import exc
from dotenv import load_dotenv
import frost_helper as helper

load_dotenv()

frost_username = os.getenv("FROST_DB_USERNAME", "")
frost_password = os.getenv("FROST_DB_PASSWORD", "")

logging_level_str = os.getenv("LOGGING_LEVEL", "ERROR")

DEFAULT_START_DATE = '2022-01-01T00:00:00Z'
START_TIME = time.time()
BASE_URL = "https://verkehr.aachen.de/Frost-Server/api/v1.1/"

# number of times an API request will be retried in case of a gateway timeout
MAX_TRIES = 5

CHARGING_STATION_VALUES = {
    'charging': 1,
    'available': 0,
    'outoforder': -1}

class verkehr_crawler():

    def query_api(self, URL: str, userpw: tuple) -> dict | None:
        """
        Generic request to FROST API.

        Parameters
        ----------
        URL : str
            viable FROST request URL
        userpw : tuple
            contains strings with FROST credentials

        Returns
        -------
        data : dict
            The FROST response to the respective URL.

        """

        bad_attempts = 0
        while bad_attempts < 6:
            try:
                response = requests.get(URL, auth=userpw)
                response.raise_for_status()  # throw error on bad query

                # Store results
                response_json = response.json()
                data = response_json
                return data

            except Exception as e:
                bad_attempts += 1
                logging.warning(e)
                time.sleep(3)
                continue

        logging.error(f"Could not fetch stuff from FROST API for URL: {URL}")
        return None


    def crawl_things(
            self,
            BASE_URL: str,
            ds_list: list,
            userpw: tuple) -> tuple:
        """
        Crawls FROST Things connected to the datastreams in the database


        Parameters
        ----------
        BASE_URL : str
            Basic database URL without request extensions
        ds_list : list[int]
            list of integers with datastream ids
        userpw : tuple
            contains strings with FROST credentials

        Returns
        -------
        thing_ids : list[int]
            list of thing IDs coresponding in structure to the datastream list
        namelist : list[string]
            list of thing names coresponding in structure to the datastream list
        lane_speedlimit : list[int]
            list of speed limits coresponding in structure to the fahrspuren table
        """

        ds_id_dict = helper.lookup_id_dict(
            table_name="datastreams",
            external_ids=ds_list)

        namelist = []
        confidential_list = []
        rows_things = []
        rows_chargingstations = []
        rows_parking = []
        rows_traffic = []

        for ds in ds_list:
            next_url = 'Thing?$select=@iot.id,name,description,properties'
            things_url = 'Datastreams(' + str(ds) + ')/' + next_url

            try:

                # query API for thing
                logging.info(f'Query_api request: {BASE_URL + things_url}')
                thing = self.query_api(BASE_URL + things_url, userpw)

                # continue if something went wrong crawling
                if not thing:
                    continue

                # new dataframe row
                rows_things.append({
                    "thing_id": thing.get('@iot.id', ''),
                    "ds_id": ds})

                spec = thing['properties'].get('species', 'unknown')

                if spec == "Ladestation":
                    rows_chargingstations.append(
                        helper.fetch_things_charger(thing))
                    namelist.append(thing.get('description', ''))
                    confidential_list.append(True)

                elif spec == "Zaehlstelle":
                    rows_traffic.append(
                        helper.fetch_things_traffic(ds, thing))
                    namelist.append(thing['properties'].get(
                        'props', {}).get('label', ''))
                    confidential_list.append(False)

                elif (spec == "Parkhaus" or spec == "Parkplatz"):
                    rows_parking.append(
                        helper.fetch_things_parking(thing, 'parkhaus'))
                    namelist.append(thing.get('name', ''))
                    if spec == "Parkhaus":
                        confidential_list.append(True)
                    else:
                        confidential_list.append(False)

                elif thing['properties'].get('type') == 'ParkingLocation':
                    rows_parking.append(
                        helper.helper.fetch_things_parking(thing, 'location'))
                    namelist.append(thing.get('name', ''))
                    confidential_list.append(False)

                elif spec == 'Parkfläche':
                    rows_parking.append(
                        helper.fetch_things_parking(thing, 'fläche'))
                    namelist.append(thing.get('name', ''))
                    confidential_list.append(False)

                else:
                    typ = thing.get('properties', {}).get(
                        'species', 'no thing species')
                    logging.info(f'unknown thing species: {typ}')
                    if thing['properties'].get('type') == 'ParkingLocation':
                        namelist.append(thing.get('name', ''))
                        confidential_list.append(False)
                    else:
                        namelist.append('')
                        typ = thing.get('properties', {}).get(
                            'species', 'no thing species')
                        logging.info(f'unknown thing species: {typ}')
                        confidential_list.append(True)
            except:
                continue

        things = pd.DataFrame(
            rows_things,
            columns=["thing_id", "ds_id"])


        traffic = pd.DataFrame(
            rows_traffic,
            columns=["datastream_id", "lane_speedlimit"])

        namelist = pd.Series(namelist)
        confidential = pd.Series(confidential_list)
        return namelist, things, rows_chargingstations, rows_parking, traffic, confidential


    def update_structure(
            self,
            datastreams: pd.DataFrame,
            ds_trafficlanes: list,
            ds_geometry: pd.DataFrame,
            userpw: tuple):
        """
        Updates sensors, datastreams and the related, more specialized tables
        to include new datastreams in the FROST database.

        Parameters
        ----------
        datastreams : pd.DataFrame
            Table of new datastreams.
        ds_trafficlanes : list
            List of dictionarys with trafficlane specifications for the new 
            datastreams.
        ds_geometry : pd.DataFrame
            Table of geometries relating to the new datastreams.
        userpw : tuple
            Username and password for accessing the FROST database.

        Returns
        -------
        None.

        """

        try:
            namelist, \
            things, \
            rows_chargingstations, \
            rows_parking, \
            traffic_things, \
            confidential = self.crawl_things(
                BASE_URL=BASE_URL,
                ds_list=datastreams["ds_id"].to_list(),
                userpw=userpw)

        except Exception as e:
            logging.error(f"Something went wrong crawling things: {e}")

        sensor_id_dict = helper.lookup_id_dict(
            table_name="sensors",
            external_ids=things["thing_id"].to_list())
        new_sensors = set(things["thing_id"]) - set(sensor_id_dict['ex_id'])

        try:
            num_duplicate, table_sensors = helper.make_sensor_table(
                things=things.loc[things["thing_id"].isin(new_sensors)],
                descriptions=namelist[things["thing_id"].isin(new_sensors)],
                geometry=ds_geometry.loc[ds_geometry["ds_id"].isin(things.loc[things["thing_id"].isin(new_sensors), "ds_id"])],
                confidential=confidential[things["thing_id"].isin(new_sensors)])
        except Exception as e:
            logging.error(f"Something went wrong creating sensor table: {e}")

        helper.feed_table_gpd(tablename='sensors', data=table_sensors.loc[table_sensors["ex_id"].isin(new_sensors)])

        chargingstations = helper.make_chargingstations_table(
            sensor_id_dict = sensor_id_dict,
            rows_chargingstations=rows_chargingstations,
            use_sensors = new_sensors)

        parking = helper.make_parking_table(rows_parking=rows_parking,
                                     use_sensors = new_sensors)

        thing_id_dict = helper.lookup_id_dict(
            table_name='Sensors',
            external_ids=things['thing_id'].to_list())

        try:
            num_duplicates, table_datastreams = helper.make_ds_table(
                thing_id_dict=thing_id_dict,
                things=things,
                datastreams=datastreams,
                confidential=confidential)
        except Exception as e:
            logging.error(f"Something went wront creating datastreams table: {e}")


        helper.feed_table_pd(tablename='datastreams',
                      data=table_datastreams)

        # error handling
        try:
            ds_trafficlanes = pd.DataFrame(ds_trafficlanes)
            table_trafficlanes = helper.make_traffic_table(    # Setzt externe statt interne IDs
                traffic_things=traffic_things,
                traffic_datastreams=ds_trafficlanes[
                    ds_trafficlanes['lane_ds_ID'].isin(datastreams['ds_id'])])
        except Exception as e:
            logging.error(f"Something went wrong creating traffic table: {e}")

        helper.feed_table_pd(tablename='trafficlanes',
                      data=table_trafficlanes)
        helper.feed_table_pd(tablename="chargingstations",
                      data=chargingstations[["type",
                                             "max_power",
                                             "sensor_id"]])
        helper.feed_table_pd(tablename="parking",
                      data=parking[["sensor_id",
                                    "type",
                                    "capacity"]])


    def crawl_and_feed_datastreams(self, BASE_URL: str, userpw: tuple) -> list[int]:
        """
        Crawls all FROST Datastreams in the FROST database and converts 
        them into timescale-db format by combining information from both
        FROST Datastreams and FROST Things

        Parameters
        ----------
        BASE_URL : str
            Basic database URL without request extensions
        userpw : tuple[str,str]
            tuple containing strings with FROST credentials

        Returns
        -------
        list[int]
            list of all relevant datastream ids

        """
        rows_trafficlane = []
        rows_datastreams = []
        rows_geometry = []

        next_url = 'Datastreams?$top=1000&$orderby=@iot.id asc&$select=@iot.id,description,properties,observedArea'

        # frost sends new URL if more than 1000 points are returned
        # if not there is no URL, so as long as there are new URLs
        # there are new datapoints
        while next_url:

            # get datastreams from API
            logging.info(f'Query_api request: {BASE_URL + next_url}')
            query_result = self.query_api(BASE_URL + next_url, userpw)

            # if there are no datastreams exit loop
            if not query_result:
                break

            # get datastream values
            datastreams = query_result.get('value')

            # exit loop if there are no datastream values
            if not datastreams:
                logging.error("No values in datastreams!")
                break

            # iterate over datastream values
            for datastream in datastreams:

                # infer klasse from datastream
                current_klasse = helper.get_klasse_from_datastream(datastream)

                # if property "Fahrspur" exists, append it to traffic lanes
                if datastream['properties'].get('Fahrspur'):
                    traffic_lane_entry = helper.create_trafficlanes_entry(datastream)
                    rows_trafficlane.append(traffic_lane_entry)  # nutzt noch externe ids

                # append metadata to rows
                rows_datastreams.append({
                    "ds_id": datastream["@iot.id"],
                    "description": datastream["description"],
                    "klasse": current_klasse})

                # retrieve coordinates
                coordinates = helper.get_coordinates_from_datastream(datastream)
                rows_geometry.append(coordinates)

            # retrieve next URL
            next_url = query_result.get('@iot.nextLink')
            if next_url:
                next_url = next_url.split('/', 6)[6]

        # creata dataframes for datastraems and geometries
        datastreams = pd.DataFrame(rows_datastreams)
        geometry = pd.DataFrame(rows_geometry)

        # look up internal IDs for external IDs
        ds_id_dict = helper.lookup_id_dict(
            table_name="datastreams",
            external_ids=datastreams["ds_id"].astype("string").to_list())
        ds_id_dict['ex_id'] = pd.to_numeric(ds_id_dict['ex_id'])
        # filter for new datastreams
        new_datastreams = set(datastreams['ds_id']) - set(ds_id_dict['ex_id'])

        # write new things and stuff for new datastreams into
        # their respective tables
        if new_datastreams:
            self.update_structure(
                datastreams=datastreams[datastreams['ds_id'].isin(new_datastreams)],
                ds_trafficlanes=rows_trafficlane,
                ds_geometry=geometry,
                userpw=userpw)

        # updated dictionary with new datastreams
        ds_id_dict = helper.lookup_id_dict(
            table_name="datastreams",
            external_ids=datastreams["ds_id"].astype("string").to_list())
        return ds_id_dict

    def crawl_and_feed_observations(self, BASE_URL: str, ds_list: list[int], userpw: tuple[str, str]):
        """Crawls all FROST Observations connected to the Datastreams in the
        database, starting from the date of the most recent observation
        connected to each specific datastream in the timescale db


        Parameters
        ----------
        BASE_URL : str
            Basic database URL without request extensions
        ds_list : list[int]
            list of integers with datastream ids
        userpw : tuple[str,str]
            tuple containing strings with FROST credentials

        Returns
        -------
        None.

        """

        # for ds in range(1, 3):     # Alternativer Header für Debugging
        # observationen werden nach Datastreams in Blöcken von maximal 1000 abgefragt und verarbeitet
        for ex_id in ds_list["ex_id"].to_list():
            obs_ID = []
            obs_DS_ID = []
            obs_results = []
            obs_timestamp = []
            confidential = []

            # fetch internal ID for external ID from Frost
            try:
                internal_id = helper.external_to_internal('datastreams', ex_id)
            except Exception as e:
                msg = f"Something went wrong converting external ID {ex_id} to internal ID"
                logging.warning(msg)

            # convert external ID to integer
            try:
                ex_id = int(ex_id)
            except Exception as e:
                logging.warning(msg)

            # get start date
            start_date = helper.get_starttime(ex_id, DEFAULT_START_DATE)

            # look up if data is confidential
            confidential_value = helper.is_confidential(internal_id, "datastreams")

            next_url = f'Observations?$top=1000&$orderby=phenomenonTime asc&$select=@iot.id,phenomenonTime,result&$filter=resultTime gt {start_date}'
            logging.info(f'Beginning to crawl observations for datastream {ex_id} from {start_date}')

            while next_url != '':
                obsURL = f'Datastreams({ex_id})/{next_url}'
                try:
                    # query frost API
                    query_result = self.query_api(BASE_URL + obsURL, userpw)
                    if not query_result:
                        continue

                    # get actual values from query result
                    observations = query_result.get('value')
                    if not observations:
                        break

                    # iterate over observations
                    for observation in observations:
                        obs_DS_ID.append(internal_id)
                        obs_ID.append(observation['@iot.id'])

                        # change possible charging stations values to integers
                        value = observation["result"]
                        observation_value = CHARGING_STATION_VALUES.get(value, value)
                        obs_results.append(observation_value)

                        obs_timestamp.append(observation['phenomenonTime'])
                        confidential.append(confidential_value)

                except Exception as e:
                    msg = f"Something went wrong crawling observations for URL {next_url}: {e}"
                    logging.warning(msg)
                    continue

                next_url = query_result.get('@iot.nextLink', '')
                if next_url:
                    next_url = next_url.split('/', 7)[7]

            # build DataFrame with observation data
            obsDictionary = {
                'datastream_id': obs_DS_ID,
                'value': obs_results,
                'timestamp': obs_timestamp,
                'confidential': confidential}
            obsFrame = pd.DataFrame(data=obsDictionary)

            # measurements will be converted to numbers
            # not numerical measurements will be dropped
            obsFrame['value'] = pd.to_numeric(obsFrame['value'], errors='coerce')
            length_before = len(obsFrame)
            obsFrame = obsFrame.dropna(subset=['value'])
            length_after = len(obsFrame)
            logging.info(f'Removed {length_before-length_after} non-numeric results from observations!')

            # drop duplicates
            length_before = len(obsFrame)
            obsFrame = obsFrame.drop_duplicates()
            length_after = len(obsFrame)
            logging.info(f'Removed {length_before-length_after} duplicate results from observations!')

            # try to feed table into database
            for max_tries in range(MAX_TRIES):
                try:
                    result = helper.feed_table_pd(tablename="measurements", data=obsFrame)
                    break
                except exc.PendingRollbackError as e:
                    logging.warning(f'Rollback Error. Trying again: {e}')
                    time.sleep(3)
                    continue

        return result


def main():

    while True:
        start_time = time.time()

        vc = verkehr_crawler()

        user = frost_username
        password = frost_password
        userpw = (user, password)

        try:
            ds_id_dict = vc.crawl_and_feed_datastreams(BASE_URL, userpw)
            logging.info('Successfully crawled datastreams!')
        except Exception as e:
            logging.error(f"Error crawling and feeding datastreams: {e}")

        if not isinstance(ds_id_dict, pd.DataFrame):
            logging.error("Could not retrieve ds_id_dict!")
            time.sleep(90)
            continue

        result = vc.crawl_and_feed_observations(BASE_URL, ds_id_dict, userpw)
        if result == 1:
            logging.info('Successfully crawled observations!')
        elif result == None:
            logging.error("Could not crawl and feed observations!")
            time.sleep(90)
            continue

        end_time = time.time()
        sleep_time = 60*60 - (end_time-start_time)
        logging.info(f"Logging new data in {sleep_time} seconds")
        time.sleep(sleep_time)


if __name__ == '__main__':

    if logging_level_str == "INFO":
        logging_level = logging.INFO
    elif logging_level_str == "WARNING":
        logging_level = logging.WARNING
    elif logging_level_str == "ERROR":
        logging_level = logging.ERROR

    logging.basicConfig(
        level=logging_level,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S')
    main()
