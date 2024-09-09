import logging

import utils
import graphing

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

karte_tab, timeline_tab = st.tabs(["Karte", "Zeitreihe"])

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
        tooltip_text = "Gesamt: {num_stations}\n"
        tooltip_text += "Frei: {free_stations}\n"
        tooltip_text += "Leistung: {max_power}kW {power_type}\n"
        tooltip_text += "{timestamp_local_str}"
        charging_deck = graphing.create_scatter_pydeck(
            data=charging_mapdata,
            tooltip={"text": tooltip_text})

        # display pydeck
        st.pydeck_chart(charging_deck)
