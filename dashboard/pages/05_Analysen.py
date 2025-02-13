
import utils

import streamlit as st
import pandas as pd
import plotly.express as px


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


weekdays_map = {
    'Monday': 1,
    'Tuesday': 2,
    'Wednesday': 3,
    'Thursday': 4,
    'Friday': 5,
    'Saturday': 6,
    'Sunday': 7}
weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

charging_tab = st.tabs(["Ladesäulen"])[0]

with charging_tab:
    sql = f"""
    SELECT
        TO_CHAR(timestamp::timestamp, 'Day') AS weekday,
        EXTRACT(HOUR FROM timestamp::timestamp) AS hour_of_day,
        AVG(hourly_usage_percent) AS mean_value
    FROM
        temporary.ladesaeulen_usage_hourly
    GROUP BY
        weekday, hour_of_day
    ORDER BY
        weekday, hour_of_day;
    """
    average_week = pd.read_sql(sql=sql, con=st.session_state["engine"])
    average_week["dayofweek"] = average_week["weekday"].map(weekdays_map)
    average_week["mean_value"] *= 100

    daily_fig = px.line(
        average_week.sort_values(["dayofweek", "hour_of_day"]),
        x='hour_of_day',
        y='mean_value',
        color='weekday',
        labels={'hour_of_day': 'Stunde des Tages', 'mean_value': 'Mittlere Auslastung in %', 'weekday': 'Wochentag'},
        title='Mittlere Auslastung nach Stunde und Wochentag',
        category_orders=None)

    st.plotly_chart(daily_fig)
