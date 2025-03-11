import os
import logging

import pydeck as pdk
import pandas as pd
import streamlit as st
import plotly.express as px


try:
    latitude = float(os.getenv("VIEWSTATE_LAT"))

except Exception as e:
    try:
        latitude = float(os.getenv("LATITUDE", 50.7751))
    except Exception as e:
        logging.error(f"Could not convert LATITUDE to float: {e}")
        latitude = 50.7751

try:
    longitude = float(os.getenv("VIEWSTATE_LONG"))
except Exception as e:
    try:
        longitude = float(os.getenv("LONGITUDE", 6.0838))
    except Exception as e:
        logging.error(f"Could not convert VIEWSTATE_LONG to float: {e}")
        longitude = 6.0838

try:
    zoom = float(os.getenv("VIEWSTATE_ZOOM", 13))
except Exception as e:
    logging.error(f"Could not convert VIEWSTATE_ZOOM to float: {e}")
    zoom = 13
try:
    pitch = float(os.getenv("VIEWSTATE_PITCH", 0))
except Exception as e:
    logging.error(f"Could not convert VIEWSTATE_PITCH to float: {e}")
    pitch = 0


def create_viewstate():
    """
    Creates viewstate
    """

    viewstate = pdk.ViewState(
        latitude=latitude,
        longitude=longitude,
        zoom=zoom,
        pitch=pitch)

    return viewstate


def create_scatter_pydeck(
        data: pd.DataFrame,
        tooltip: dict[str, str] | None = None):
    """
    Creates pydeck with ScatterPlotlayer

    Parameters:
    -------------
    data: pd.DataFrame
        The data to create the pydeck for

    tooltip: dict[str, str]
        The tooltip to display on the pydeck
    """

    layer = pdk.Layer(
        type="ScatterplotLayer",
        data=data,
        get_position=["longitude", "latitude"],
        get_fill_color="color",
        get_radius=st.session_state["point_size"],
        pickable=True)

    viewstate = create_viewstate()

    deck = pdk.Deck(
        tooltip=tooltip,
        map_style=st.session_state["map_style"]["style"],
        map_provider=st.session_state["map_style"]["provider"],
        initial_view_state=viewstate,
        layers=[layer])

    return deck


def create_path_pydeck(
        data: pd.DataFrame,
        tooltip: dict[str, str] | None = None):
    """
    Creates pydeck with PathLayer

    Parameters:
    -------------
    data: pd.DataFrame
        The data to create the pydeck for

    tooltip: dict[str, str]
        The tooltip to display on the pydeck
    """

    layer = pdk.Layer(
        type="PathLayer",
        data=data,
        pickable=True,
        width_scale=10,
        get_width_pixels=5,
        get_path="geometry",
        get_color="color")

    viewstate = create_viewstate()

    deck = pdk.Deck(
        tooltip=tooltip,
        map_style=st.session_state["map_style"]["style"],
        map_provider=st.session_state["map_style"]["provider"],
        initial_view_state=viewstate,
        layers=[layer])

    return deck


def create_linefig(
        data: pd.DataFrame,
        x: str = "timestamp_local",
        y: str = "value",
        color: str = "datastream_id",
        title: str = "",
        ylabel: str | None = None,
        legend_title: str | None = None,
        is_bike: bool = False):

    data = data.copy()
    data.sort_values(["datastream_id", "timestamp_local"], inplace=True)

    fig = px.line(
        data_frame=data,
        x=x, y=y,
        color=color)

    if is_bike:
        title=data["description"].values[0]

    fig.update_layout(
        title=title,
        xaxis_title="", yaxis_title=ylabel,
        legend_title=legend_title)

    return fig
