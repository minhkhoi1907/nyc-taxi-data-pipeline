import streamlit as st
from app.utils.db import execute_query

st.set_page_config(page_title="Traffic Efficiency", layout="wide")

st.title("Traffic Efficiency")
st.markdown("Source Data Mart: `mart_traffic_efficiency`")

df_traffic = execute_query("SELECT * FROM mart_traffic_efficiency ORDER BY avg_duration_minutes DESC LIMIT 100")

if df_traffic is None or df_traffic.empty:
    st.warning("No data found in mart_traffic_efficiency.")
    st.stop()

st.subheader("Longest Routes by Duration (Top 10)")

top_10 = df_traffic.head(10)

if 'pickup_location_id' in top_10.columns and 'dropoff_location_id' in top_10.columns:
    top_10['Route'] = "PU: " + top_10['pickup_location_id'].astype(str) + " -> DO: " + top_10['dropoff_location_id'].astype(str)
    st.bar_chart(top_10, x="Route", y="avg_duration_minutes", color="#f0a500")

st.divider()

st.subheader("Traffic Performance Summary")
st.dataframe(df_traffic, use_container_width=True)
