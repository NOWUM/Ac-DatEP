import streamlit as st

import logging

def convert_save_mapstyle(raw_mapstyle: str | None):
    """
    Converts choosen mapstyle value and saves it in session state
    """

    logging.info("Trying to convert mapstyle")

    try:
        if raw_mapstyle == "Auto":
            st.session_state["map_style"] = {"style": None, "provider": None}
        elif raw_mapstyle == "Hell":
            st.session_state["map_style"] = {"style": "light", "provider": "mapbox"}
        elif raw_mapstyle == "Dunkel":
            st.session_state["map_style"] = {"style": "dark", "provider": "mapbox"}
        elif raw_mapstyle == "Satellit":
            st.session_state["map_style"] = {"style": "satellite", "provider": "mapbox"}
        elif raw_mapstyle == "Farbe":
            st.session_state["map_style"] = {"style": "mapbox://styles/mapbox/outdoors-v12", "provider": "mapbox"}
        elif raw_mapstyle == None:
            pass

        logging.info("Succesfully converted mapstyle")

    except Exception as e:
        logging.error(f"Could not convert mapstyle '{raw_mapstyle}': {e}")
        return {"style": None, "provider": None}


def choose_mapstyle():
    """
    Displays, converts and saves mapstyle information
    """

    # set index of mapstyle
    if not "map_style" in st.session_state:
        convert_save_mapstyle('Auto')

    # mapstyle selection
    raw_mapstyle = st.selectbox(
        label="Kartendarstellung",
        options=["Auto", "Hell", "Dunkel", "Satellit", "Farbe"],
        index=None)

    convert_save_mapstyle(raw_mapstyle)


def choose_point_size():

    if not "point_size" in st.session_state:
        st.session_state["point_size"] = 30

    point_size = st.slider(
        label="Punktgröße",
        min_value=1,
        max_value=300,
        value=30,
        step=1)

    st.session_state["point_size"] = point_size


def display_sidebar():
    """
    Displays sidebar with page navigation, settings and info.
    """

    with st.sidebar:

        # pages
        st.header("Seiten")
        st.page_link("Home.py", label="Home")
        st.page_link("pages/01_Luftqualität.py", label="Luft")
        st.page_link("pages/02_Verkehr.py", label="Verkehr")
        st.page_link("pages/03_Parken.py", label="Parken")
        st.page_link("pages/04_Ladesäulen.py", label="E-Ladesläulen")
        st.write("-----------------------")

        # settings
        st.header("Einstellungen")

        # mapstyle
        choose_mapstyle()

        # point size
        choose_point_size()
