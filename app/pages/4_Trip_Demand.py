import streamlit as st
from app.utils.db import execute_query

st.set_page_config(page_title="Trip Demand", layout="wide")

st.title("Trip Demand Analytics")
st.markdown("Source Data Mart: `mart_trip_demand`")

df_demand = execute_query("SELECT * FROM mart_trip_demand")

if df_demand is None or df_demand.empty:
    st.warning("No data found in mart_trip_demand.")
    st.stop()

st.subheader("Demand Matrix Details")
st.dataframe(df_demand, use_container_width=True)

if 'hour' in df_demand.columns and 'total_trips' in df_demand.columns:
    st.subheader("Volume Distribution by Hour")
    hourly_demand = df_demand.groupby('hour')['total_trips'].sum().reset_index()
    st.line_chart(hourly_demand, x="hour", y="total_trips", color="#A020F0")

if 'day_of_week' in df_demand.columns and 'total_trips' in df_demand.columns:
    st.subheader("Volume Distribution by Day of Week")
    daily_demand = df_demand.groupby('day_of_week')['total_trips'].sum().reset_index()
    st.bar_chart(daily_demand, x="day_of_week", y="total_trips", color="#00ff00")
