import utils
import graphing

import streamlit as st
import logging


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

logging.info("Filtering parkobjekte datastreams")
parking_datastreams = utils.filter_dataframe(
    dataframe=st.session_state["datastreams"],
    filter_column="type",
    _filter=["Parkobjekt"],
)

parking_sensor_ids = parking_datastreams["sensor_id"].to_list()
parking_sensors = utils.filter_dataframe(
    dataframe=st.session_state["sensors"],
    filter_column="id",
    _filter=parking_sensor_ids,
)

parking_mapdata = utils.fetch_prepare_measurements(
    datastreams=parking_datastreams,
    sensors=parking_sensors,
)


map_tab, ts_tab, forecast_tab = st.tabs(["Karte", "Zeitreihe", "Forecast"])

with map_tab:
    if parking_mapdata.empty:
        utils.display_no_data_warning()

    else:
        max_value = 300 # max free places for colorscale
        parking_mapdata["adjusted_value"] = max_value - parking_mapdata["value"]
        utils.add_color_to_data(
            data=parking_mapdata,
            value_col="adjusted_value",
            min_value=0,
            max_value=max_value,
            colorscale="Plasma"
        )

        tooltip = {"text":
                """Parkhaus {description}
                Freie Plätze: {value}
                {timestamp_local_str}"""}
        parking_deck = graphing.create_scatter_pydeck(
            data=parking_mapdata,
            tooltip=tooltip
        )
        st.pydeck_chart(parking_deck)

with ts_tab:

    # get user input for timeframe and convert to viewname
    viewname = utils.get_viewname_from_user_input(
        label="pm10_timeframe",
        agg_type="avg")

    # query timeseries measurements
    parking_tsdata = utils.fetch_prepare_measurements(
        datastreams=parking_datastreams,
        sensors=parking_sensors,
        viewname=viewname)
    
    if parking_tsdata.empty:
        utils.display_no_data_warning()
    else:

        # create figure
        pm10_fig = graphing.create_linefig(
            data=parking_tsdata,
            color="description",
            ylabel="Freie Plätze")

        # display
        st.plotly_chart(pm10_fig)
