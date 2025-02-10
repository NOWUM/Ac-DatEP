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

st.success("**Disclaimer:** Those values not obtained from LANUV use low cost sensors and may not always be accurate!")

# create tabs
pm10_tab, pm25_tab, temperature_tab, co2_tab = st.tabs([
    "10µm-Feinstaub", "2.5µm-Feinstaub", "Temperatur", "CO₂"])

# tooltip is same for alle maps, so defining it here
tooltip = {"text":
           """Datastream ID: {datastream_id}
           Wert: {value} µg/m³
           {timestamp_local_str}
           Quelle: {source}"""}

with pm10_tab:

    logging.info("Filtering PM10 datastreams")
    # filter datastreams for particles
    pm10_datastreams = utils.filter_dataframe(
        dataframe=st.session_state["datastreams"],
        filter_column="type",
        _filter=["PM10"])

    logging.info("Filtering PM10 sensor ids")
    # query sensors for these datastreams
    pm10_sensor_ids = pm10_datastreams["sensor_id"].to_list()
    pm10_sensors = utils.filter_dataframe(
        dataframe=st.session_state["sensors"],
        filter_column="id",
        _filter=pm10_sensor_ids)

    pm10_mapdata = utils.fetch_prepare_measurements(
        datastreams=pm10_datastreams,
        sensors=pm10_sensors)

    if pm10_mapdata.empty:
        utils.display_no_data_warning()

    else:
        # add color to measurements
        utils.add_color_to_data(
            data=pm10_mapdata,
            min_value=0,
            max_value=50,
            colorscale="Plasma")

        # create map
        pm10_deck = graphing.create_scatter_pydeck(
            data=pm10_mapdata,
            tooltip=tooltip)
        st.pydeck_chart(pm10_deck)

        st.markdown("----")

        # get user input for timeframe and convert to viewname
        viewname = utils.get_viewname_from_user_input(
            label="pm10_timeframe",
            agg_type="avg")

        # query timeseries measurements
        pm10_tsdata = utils.fetch_prepare_measurements(
            datastreams=pm10_datastreams,
            sensors=pm10_sensors,
            viewname=viewname)

        # create figure
        pm10_fig = graphing.create_linefig(
            data=pm10_tsdata,
            ylabel="Feinstaub > 10μm in μg/m³")

        # display
        st.plotly_chart(pm10_fig)


with pm25_tab:

    logging.info("Filtering PM25 datastreams")
    # filter datastreams for particles
    pm25_datastreams = utils.filter_dataframe(
        dataframe=st.session_state["datastreams"],
        filter_column="type",
        _filter=["PM2.5"])

    logging.info("querying PM 25 sensor ids")
    # query sensors for these datastreams
    pm25_sensor_ids = pm25_datastreams["sensor_id"].to_list()
    pm25_sensors = utils.filter_dataframe(
        dataframe=st.session_state["sensors"],
        filter_column="id",
        _filter=pm25_sensor_ids)

    pm25_mapdata = utils.fetch_prepare_measurements(
        datastreams=pm25_datastreams,
        sensors=pm25_sensors)

    if pm25_mapdata.empty:
        utils.display_no_data_warning()

    else:
        # add color to measurements
        utils.add_color_to_data(
            data=pm25_mapdata,
            min_value=0,
            max_value=50,
            colorscale="Plasma")

        # create map
        pm25_deck = graphing.create_scatter_pydeck(
            data=pm25_mapdata,
            tooltip=tooltip)
        st.pydeck_chart(pm25_deck)

        st.markdown("----")

        # get user input for timeframe and convert to viewname
        viewname = utils.get_viewname_from_user_input(
            label="pm25_timeframe",
            agg_type="avg")

        # query timeseries measurements
        pm25_tsdata = utils.fetch_prepare_measurements(
            datastreams=pm25_datastreams,
            sensors=pm10_sensors,
            viewname=viewname)

        # create figure
        pm25_fig = graphing.create_linefig(
            data=pm25_tsdata,
            ylabel="Feinstaub > 2.5μm in μg/m³")

        # display
        st.plotly_chart(pm25_fig)

with temperature_tab:

    logging.info("Filtering temperature datastreams")
    # filter datastreams for particles
    temperature_datastreams = utils.filter_dataframe(
        dataframe=st.session_state["datastreams"],
        filter_column="type",
        _filter=["temperature"])

    logging.info("Filtering temperature sensor ids")
    # query sensors for these datastreams
    temperature_sensor_ids = temperature_datastreams["sensor_id"].to_list()
    temperature_sensors = utils.filter_dataframe(
        dataframe=st.session_state["sensors"],
        filter_column="id",
        _filter=temperature_sensor_ids)

    temperature_mapdata = utils.fetch_prepare_measurements(
        datastreams=temperature_datastreams,
        sensors=temperature_sensors)

    if temperature_mapdata.empty:
        utils.display_no_data_warning()

    else:
        # add color to measurements
        utils.add_color_to_data(
            data=temperature_mapdata,
            min_value=-20,
            max_value=50,
            colorscale="Plasma")

        # create map
        temperatue_deck = graphing.create_scatter_pydeck(
            data=temperature_mapdata,
            tooltip=tooltip)
        st.pydeck_chart(temperatue_deck)

        st.markdown("----")

        # get user input for timeframe and convert to viewname
        viewname = utils.get_viewname_from_user_input(
            label="temperature_timeframe",
            agg_type="avg")

        # query timeseries measurements
        temperature_tsdata = utils.fetch_prepare_measurements(
            datastreams=temperature_datastreams,
            sensors=temperature_sensors,
            viewname=viewname)

        # create figure
        temperature_fig = graphing.create_linefig(
            data=temperature_tsdata,
            ylabel="Temperatur in °C")

        # display
        st.plotly_chart(temperature_fig)


with co2_tab:

    logging.info("Filtering CO2 datastreams")
    # filter datastreams for particles
    co2_datastreams = utils.filter_dataframe(
        dataframe=st.session_state["datastreams"],
        filter_column="type",
        _filter=["CO2"])

    logging.info("Filtering CO2 sensor ids")
    # query sensors for these datastreams
    co2_sensor_ids = co2_datastreams["sensor_id"].to_list()
    co2_sensors = utils.filter_dataframe(
        dataframe=st.session_state["sensors"],
        filter_column="id",
        _filter=co2_sensor_ids)

    co2_mapdata = utils.fetch_prepare_measurements(
        datastreams=co2_datastreams,
        sensors=co2_sensors)

    if co2_mapdata.empty:
        utils.display_no_data_warning()

    else:
        # add color to measurements
        utils.add_color_to_data(
            data=co2_mapdata,
            min_value=-20,
            max_value=50,
            colorscale="Plasma")

        # create map
        co2_deck = graphing.create_scatter_pydeck(
            data=co2_mapdata,
            tooltip=tooltip)
        st.pydeck_chart(co2_deck)

        st.markdown("----")

        # get user input for timeframe and convert to viewname
        viewname = utils.get_viewname_from_user_input(
            label="co2_timeframe",
            agg_type="avg")

        # query timeseries measurements
        co2_tsdata = utils.fetch_prepare_measurements(
            datastreams=co2_datastreams,
            sensors=co2_sensors,
            viewname=viewname)

        # create figure
        co2_fig = graphing.create_linefig(
            data=co2_tsdata,
            ylabel="CO2 in μg/m³")

        # display
        st.plotly_chart(co2_fig)

with ozon_tab:

    logging.info("Filtering CO2 datastreams")
    # filter datastreams for particles
    ozon_datastreams = utils.filter_dataframe(
        dataframe=st.session_state["datastreams"],
        filter_column="type",
        _filter=["Ozon"])

    logging.info("Filtering Ozon sensor ids")
    # query sensors for these datastreams
    ozon_sensor_ids = ozon_datastreams["sensor_id"].to_list()
    ozon_sensors = utils.filter_dataframe(
        dataframe=st.session_state["sensors"],
        filter_column="id",
        _filter=ozon_sensor_ids)

    ozon_mapdata = utils.fetch_prepare_measurements(
        datastreams=ozon_datastreams,
        sensors=ozon_sensors)

    if ozon_mapdata.empty:
        utils.display_no_data_warning()

    else:
        # add color to measurements
        utils.add_color_to_data(
            data=ozon_mapdata,
            min_value=0,
            max_value=120,
            colorscale="Plasma")

        # create map
        ozon_deck = graphing.create_scatter_pydeck(
            data=ozon_mapdata,
            tooltip=tooltip)
        st.pydeck_chart(ozon_deck)

        st.markdown("----")

        # get user input for timeframe and convert to viewname
        viewname = utils.get_viewname_from_user_input(
            label="ozon_timeframe",
            agg_type="avg")

        # query timeseries measurements
        ozon_tsdata = utils.fetch_prepare_measurements(
            datastreams=ozon_datastreams,
            sensors=ozon_sensors,
            viewname=viewname)

        # create figure
        ozon_fig = graphing.create_linefig(
            data=ozon_tsdata,
            ylabel="CO2 in μg/m³")

        # display
        st.plotly_chart(ozon_fig)
