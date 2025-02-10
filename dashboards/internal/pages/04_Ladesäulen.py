import logging

import utils
import graphing

import streamlit as st
import pandas as pd


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

karte_tab, ts_tab = st.tabs(["Karte", "Zeitreihe"])

with karte_tab:

    logging.info("Filtering charging datastreams")
    # filter datastreams for particles
    charging_datastreams = utils.filter_dataframe(
        dataframe=st.session_state["datastreams"],
        filter_column="type",
        _filter=["E-Ladepunkt"])

    logging.info("Filtering charging sensor ids")
    # query sensors for these datastreams
    charging_sensor_ids = charging_datastreams["sensor_id"].to_list()
    charging_sensors = utils.filter_dataframe(
        dataframe=st.session_state["sensors"],
        filter_column="id",
        _filter=charging_sensor_ids)

    charging_mapdata = utils.fetch_prepare_measurements(
        datastreams=charging_datastreams,
        sensors=charging_sensors)

    if charging_mapdata.empty:
        utils.display_no_data_warning()

    else:
        # add additional information for charging stations
        charging_mapdata = utils.add_charging_stations_info(charging_mapdata)

        # add color
        utils.add_color_to_data(
            data=charging_mapdata,
            value_col="perct_used",
            min_value=0,
            max_value=1,
            colorscale="Bluered")

        # create pydeck
        tooltip = "{description}\n"
        tooltip += "Gesamt: {num_stations}\n"
        tooltip += "Frei: {free_stations}\n"
        tooltip += "Leistung: {max_power}kW {power_type}\n"
        tooltip += "Datastream ID: {datastream_id}\n"
        tooltip += "{timestamp_local_str}"
        charging_deck = graphing.create_scatter_pydeck(
            data=charging_mapdata,
            tooltip={"text": tooltip})

        # display pydeck
        st.pydeck_chart(charging_deck)

with ts_tab:

    charging_station_park = st.selectbox(
        label="Auswahl Ladepark",
        options=["Alle"] + charging_sensors["description"].sort_values().tolist(),
        key="charging_ts_id")

    # get user input for timeframe and convert to viewname
    viewname = utils.get_viewname_from_user_input(
        label="charging_timeframe",
        agg_type="avg")

    # query timeseries measurements
    charging_tsdata = utils.fetch_prepare_measurements(
        datastreams=charging_datastreams,
        sensors=charging_sensors,
        viewname=viewname)

    if charging_tsdata.empty:
        utils.display_no_data_warning()
    else:
        # filter by station park
        if charging_station_park == 'Alle':
            pass
        else:
            charging_tsdata = charging_tsdata[charging_tsdata["description"] == charging_station_park]

        # create figure
        charging_fig = graphing.create_linefig(
            data=charging_tsdata,
            ylabel="Auslastung\n(1=komplett belegt, 0=alle frei)",
            legend_title="Datastream ID")

        # display
        st.plotly_chart(charging_fig)
