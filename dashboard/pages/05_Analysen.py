
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
    try:
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
            title='Mittlere Auslastung im Wochenverlauf nach Wochentag')
        st.plotly_chart(daily_fig)
    except Exception as e:
        utils.display_error_message()

    try:
        sql = f"""
            SELECT
                EXTRACT(DOY FROM timestamp::timestamp) AS dayofyear,
                AVG(hourly_usage_percent) AS mean_value
            FROM
                temporary.ladesaeulen_usage_hourly
            GROUP BY
                EXTRACT(DOY FROM timestamp::timestamp)
            ORDER BY
                dayofyear;
        """
        yearly_df = pd.read_sql(sql, con=st.session_state["engine"])
        yearly_df["mean_value"] *= 100
        yearly_fig = px.bar(
            data_frame=yearly_df, x="dayofyear", y="mean_value",
            labels={"dayofyear": "Tag des Jahres", "mean_value": "Mittlere Auslastung in %"},
            title="Mittlere Auslastung - Jahresverlauf")
        st.plotly_chart(yearly_fig)
    except Exception as e:
        utils.display_error_message()

    try:
        sql = f"""
            SELECT
                datastream_id AS id,
                AVG(hourly_usage_percent) AS mean_value
            FROM
                temporary.ladesaeulen_usage_hourly
            GROUP BY
                id
            ORDER BY mean_value DESC
            LIMIT 11;
        """
        top10_df = pd.read_sql(sql, con=st.session_state["engine"])
        top10_df = pd.merge(
            left=top10_df,
            right=st.session_state["datastreams"],
            on="id")
        top10_df = pd.merge(
            left=top10_df,
            right=st.session_state["sensors"],
            left_on="sensor_id",
            right_on="id")
        st.header("Meistgenutzte Ladesäulen")
        l_col, r_col = st.columns(2)
        for idx, row in top10_df.iterrows():
            if idx + 1 < 6:
                l_col.write(f"**{idx+1}. Platz:** {row['description']} - {row['mean_value']*100:.2f}% Auslastung")
            else:
                r_col.write(f"**{idx+1}. Platz:** {row['description']} - {row['mean_value']*100:.2f}% Auslastung")
    except Exception as e:
        utils.display_error_message()

    try:
        sql = f"""
            SELECT
                avg(value) as value,
                max_power
            FROM temporary.ladesaeulen_usage_hourly
            LEFT JOIN datastreams ON temporary.ladesaeulen_usage_hourly.datastream_id = datastreams.id
            LEFT JOIN chargingstations ON chargingstations.sensor_id = datastreams.sensor_id
            GROUP BY max_power
        """
        power_df = pd.read_sql(sql, con=st.session_state["engine"])
        power_df.dropna(inplace=True)
        power_df["value"] *= 100
        power_df["max_power"] = power_df["max_power"].astype(str)
        power_df["max_power"] += "kW"
        power_fig = px.bar(
            data_frame=power_df,
            x="max_power", y="value",
            labels={"max_power": "Maximale Ladeleistung in kW", "value": "Mittlere Auslastung in %"},
            title="Mittlere Auslastung nach maximaler Ladeleistung")
        st.plotly_chart(power_fig)
    except Exception as e:
        print(e)
        utils.display_error_message()
