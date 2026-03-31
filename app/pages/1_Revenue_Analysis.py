import streamlit as st
import pandas as pd
from app.utils.db import execute_query

st.set_page_config(page_title="Revenue Analysis", layout="wide")

st.title("Revenue Analysis")
st.markdown("Source Data Mart: `mart_revenue_analysis`")

df_all = execute_query("SELECT * FROM mart_revenue_analysis")

if df_all is None or df_all.empty:
    st.warning("No data found in mart_revenue_analysis. Please execute the data pipeline.")
    st.stop()

st.subheader("Key Performance Indicators (KPIs)")
total_revenue = df_all['total_revenue'].sum() if 'total_revenue' in df_all.columns else 0
total_trips = df_all['total_trips'].sum() if 'total_trips' in df_all.columns else 0
avg_fare = df_all['avg_fare'].mean() if 'avg_fare' in df_all.columns else 0

kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Total Revenue", f"${total_revenue:,.2f}")
kpi2.metric("Total Trips", f"{total_trips:,.0f}")
kpi3.metric("Avg Fare / Trip", f"${avg_fare:,.2f}")

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Revenue by Vendor")
    if 'vendor_id' in df_all.columns and 'total_revenue' in df_all.columns:
        vendor_revenue = df_all.groupby('vendor_id')['total_revenue'].sum().reset_index()
        vendor_revenue['vendor_name'] = vendor_revenue['vendor_id'].map({1: 'Creative Mobile', 2: 'VeriFone'})
        vendor_revenue['vendor_name'] = vendor_revenue['vendor_name'].fillna('Unknown')
        
        st.bar_chart(vendor_revenue, x="vendor_name", y="total_revenue", color="#FF4B4B")
    else:
        st.info("No vendor_id data available.")

with col2:
    st.subheader("Mart Data Table")
    st.dataframe(df_all, use_container_width=True)

st.divider()

if 'month' in df_all.columns:
    st.subheader("Revenue Trend Over Months")
    st.line_chart(df_all, x="month", y="total_revenue", color="#29b5e8")
elif 'date_id' in df_all.columns:
    st.subheader("Revenue Trend Over Time")
    df_date = df_all.groupby('date_id')['total_revenue'].sum().reset_index()
    df_date['date_id'] = df_date['date_id'].astype(str)
    st.line_chart(df_date, x="date_id", y="total_revenue", color="#29b5e8")
