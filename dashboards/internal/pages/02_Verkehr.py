import logging

import utils
import graphing
import coloring

import streamlit as st


# page layout and config
st.set_page_config(
    page_title="AC-DatEP",
    initial_sidebar_state="auto",
    layout="wide",
    menu_items={
        "Report a bug": "mailto:komanns@fh-aachen.de?subject=Bug Report AC-DatEP Dashboard",
        "About": "https://www.m2c-lab.fh-aachen.de/acdatenpool/"},
    page_icon="dashboards/images/ACDAtEP.svg")

# perform default page jobs
utils.perform_default_page_jobs()

kfz_tab, bike_tab, pedestrian_tab = st.tabs(
    ["KFZ", "Fahrrad", "Fußgänger"])

with kfz_tab:

    # filter datastreams for particles
    logging.info("Filtering traffic datastreams")
    kfz_datastreams = utils.filter_dataframe(
        dataframe=st.session_state["datastreams"],
        filter_column="type",
        _filter=["level of congestion"])

    # query sensors for these datastreams
    logging.info("Filtering traffic sensor ids")
    kfz_sensor_ids = kfz_datastreams["sensor_id"].to_list()
    kfz_sensors = utils.filter_dataframe(
        dataframe=st.session_state["sensors"],
        filter_column="id",
        _filter=kfz_sensor_ids)

    # fetching measurements
    logging.info("Fetching and preparing measurements")
    kfz_mapdata = utils.fetch_prepare_measurements(
        datastreams=kfz_datastreams,
        sensors=kfz_sensors,
        is_traffic=True)

    if kfz_mapdata.empty:
        utils.display_no_data_warning()

    else:
        # add color to measurements
        logging.info("Adding color to traffic data")
        utils.add_color_to_data(
            data=kfz_mapdata,
            min_value=1,
            max_value=3,
            color_dict=coloring.KFZ_COLOR_DICT)

        # create map
        logging.info("Creating traffic deck")
        kfz_deck = graphing.create_path_pydeck(
            data=kfz_mapdata,
            tooltip={"text": "{description}\n congestion level {value}\n {timestamp_local_str}"})

        st.pydeck_chart(kfz_deck)

with bike_tab:

    # query trafficlanes
    if not "trafficlanes" in st.session_state:
        logging.info("Querying trafficlanes from DB")
        sql = "select * from trafficlanes"
        st.session_state["trafficlanes"] = utils.query_datasbase(sql)
    trafficlanes = st.session_state["trafficlanes"]

    logging.info("Filtering bike datastreams")
    bike_datastreams = utils.filter_dataframe(
        dataframe=st.session_state["datastreams"],
        filter_column="type",
        _filter=["bike traffic measurement"])

    # query sensors for these datastreams
    logging.info("Filtering bike sensors")
    bike_sensor_ids = bike_datastreams["sensor_id"].to_list()
    bike_sensors = utils.filter_dataframe(
        dataframe=st.session_state["sensors"],
        filter_column="id",
        _filter=bike_sensor_ids)

    # get viewname
    bike_viewname = utils.get_viewname_from_user_input(
        label="bike_timeframe",
        agg_type="sum")

    # query timeseries
    bike_data = utils.fetch_prepare_measurements(
        datastreams=bike_datastreams,
        sensors=bike_sensors,
        viewname=bike_viewname)

    # display no data warning if no data exists
    if bike_data.empty:
        utils.display_no_data_warning()

    else:
        # add trafficlanes info
        bike_data = utils.add_trafficlanes_info(bike_data)

        # remove everythin that is not aggregation "h"
        bike_data = bike_data[bike_data["aggregation"] == "h"].reset_index(drop=True)

        # remove everything containing "Kfz" in trafficlanes information
        bike_data = bike_data[~bike_data["lane"].str.contains("Kfz")].reset_index(drop=True)

        # create a figure for each sensor ID
        figs = bike_data.groupby("sensor_id").apply(
            func=graphing.create_linefig,
            color="lane",
            ylabel="Summe der gezählten Radfahrer",
            legend_title="Fahrtrichtung",
            is_bike=True)

        for fig in figs:
            st.plotly_chart(fig)
