import logging
import os

import utils
import graphing

import streamlit as st
import pydeck as pdk


logging_level_str = os.getenv("LOGGING_LEVEL", "ERROR")

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

# page config
st.set_page_config(
    page_title="AC-DatEP",
    initial_sidebar_state="auto",
    layout="wide",
    menu_items={
        "Report a bug": "mailto:komanns@fh-aachen.de?subject=Bug Report AC-DatEP Dashboard",
        "About": "https://www.m2c-lab.fh-aachen.de/acdatenpool/"},
    page_icon="dashboards/images/ACDAtEP.svg")

st.title("Willkommen auf dem Dashboard des Aachener Datenpools")

st.header("Was ist der Aachener Datenpool?")
st.write("Im Januar 2022 startete die FH Aachen (NOWUM-Energy, m²c-Lab) gemeinsam mit ihren Partnern bei der Stadt Aachen, cityscaper, 4traffic SET und Rupprecht Consult das im Rahmen des Förderprogramms mFUND vom Bundesministerium für Digitales und Verkehr (BMDV) unterstütze Forschungsprojekt „Aachener Datenpool“ (kurz: AC-DatEP).")
st.write(" Im Durchführungszeitraum ist die großräumige Installation echtzeitfähiger Erfassungstechnik des Verkehrsflusses sowie vieler weiterer Umweltparameter im Raum Aachen geplant. Hierfür werden ausgewählte Straßenlaternen der Stadt Aachen mit Sensorboxen ausgestattet, die neben Messwerten wie Temperatur, Lärmpegel, Luftfeuchtigkeit und -druck auch die Anzahl und Geschwindigkeit der vorbeifahrenden Fahrzeuge sowie deren Fahrzeugklassifikation aufzeichnen.")
st.write(" Ziel des Projekts ist es, die so erhobenen Daten in eine Open-Data Basis einzupflegen, die dann mit bereits existierenden Datensätzen angereichert und Dritten zugänglich gemacht werden kann. Der auf diese Weise geschaffene Datenpool birgt das Potenzial, die Grundlage für viele spannende Nutzungskonzepte zu bilden. Schon während der Projektlaufzeit sollen Start-Ups sowie etablierte Stakeholder die Möglichkeit erhalten, mit den Daten zu arbeiten und neue Geschäftsmodelle sowie Planungs- und Mobilitätskonzepte zu testen.")
st.write("Mehr Infos: https://www.aachener-datenpool.de, https://acdatep.nowum.fh-aachen.de")

st.header("Kann ich die Rohdaten einsehen?")
st.write("Selbstverständlich! Alle Daten liegen in der Datenbank des Aachener Datenpool und sind über eine REST API unter https://acdatep.nowum.fh-aachen.de/api/docs abfragbar!")

# perform default page jobs
utils.perform_default_page_jobs()


st.header("Unsere Messstellen")

map_data = utils.prepare_location_dataframe()

# prepare layer
layer = pdk.Layer(
    type="ScatterplotLayer",
    get_fill_color="color",
    data=map_data,
    get_position=["longitude", "latitude"],
    get_radius=st.session_state["point_size"],
    pickable=True)

# create default viewstate
viewstate = graphing.create_viewstate()

# tooltip for pydeck
tooltip = {"text": "Quelle: {source}\n Position: {latitude}N, {longitude}E"}

# the deck
deck = pdk.Deck(
    tooltip=tooltip,
    map_style=st.session_state["map_style"]["style"],
    map_provider=st.session_state["map_style"]["provider"],
    initial_view_state=viewstate,
    layers=[layer])
st.pydeck_chart(deck)
