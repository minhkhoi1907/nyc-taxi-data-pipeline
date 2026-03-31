import streamlit as st
from app.utils.db import execute_query

st.set_page_config(page_title="Customer Segmentation", layout="wide")

st.title("Customer Segmentation")
st.markdown("Source Data Mart: `mart_customer_segmentation`")

df_cust = execute_query("SELECT * FROM mart_customer_segmentation")

if df_cust is None or df_cust.empty:
    st.warning("No data found in mart_customer_segmentation.")
    st.stop()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Payment Type Matrix")
    if 'payment_type' in df_cust.columns and 'total_trips' in df_cust.columns:
        pie_data = df_cust.groupby('payment_type')['total_trips'].sum().reset_index()
        st.bar_chart(pie_data, x="payment_type", y="total_trips", color="#83c5be")
    else:
        st.info("No payment_type data available.")

with col2:
    st.subheader("Tipping Behavior vs Fare Amount")
    if 'fare_amount' in df_cust.columns and 'avg_tip_amount' in df_cust.columns:
        st.scatter_chart(df_cust, x="fare_amount", y="avg_tip_amount", color="#ffcdb2")
    else:
        st.info("Insufficient data for correlation chart (requires fare_amount and avg_tip_amount).")

st.divider()

st.subheader("Segmentation Matrix Details")
st.dataframe(df_cust, use_container_width=True)
