import streamlit as st
import duckdb
import pandas as pd
import os

# --- Page Config ---
st.set_page_config(
    page_title="NYC Taxi Pipeline Dashboard",
    page_icon="🚕",
    layout="wide"
)

# --- Define Paths ---
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT_DIR, "dev.duckdb")

# --- Connect to DuckDB ---
@st.cache_resource
def get_connection():
    if not os.path.exists(DB_PATH):
        st.error(f"❌ Không tìm thấy database tại {DB_PATH}. Hãy chạy pipeline trước!")
        st.stop()
    # Read-only mode để an toàn khi dev dbt đồng thời
    return duckdb.connect(DB_PATH, read_only=True)

conn = get_connection()

# --- Load Data ---
@st.cache_data
def load_revenue_data():
    try:
        return conn.execute("SELECT * FROM mart_revenue_analysis").df()
    except Exception as e:
        return pd.DataFrame()

@st.cache_data
def load_traffic_data():
    try:
        return conn.execute("SELECT * FROM mart_traffic_efficiency").df()
    except Exception as e:
        return pd.DataFrame()

# --- UI Setup ---
st.title("🚕 NYC Taxi Data Pipeline - Medallion Architecture")
st.markdown("""
Dashboard hiển thị dữ liệu đã được xử lý thông qua luồng: 
**HDFS -> Apache Spark -> DuckDB -> dbt -> Streamlit**
""")

revenue_df = load_revenue_data()
traffic_df = load_traffic_data()

if revenue_df.empty or traffic_df.empty:
    st.warning("⚠️ Chưa có dữ liệu trong DuckDB Marts. Vui lòng chạy `python scripts/run_pipeline.py` trước.")
    st.stop()

# --- Layout: Key Metrics ---
st.header("1. 💰 Tổng quan Doanh thu (Revenue Analysis)")
col1, col2, col3 = st.columns(3)

total_revenue = revenue_df['total_revenue'].sum()
total_trips = revenue_df['total_trips'].sum()
avg_fare = revenue_df['avg_fare'].mean()

col1.metric("Tổng Doanh Thu ($)", f"${total_revenue:,.0f}")
col2.metric("Tổng Số Chuyến", f"{total_trips:,.0f}")
col3.metric("Giá Cước TB/Chuyến", f"${avg_fare:,.2f}")

st.divider()

# --- Layout: Charts ---
# Biểu đồ thanh: Doanh thu theo tháng
st.subheader("Doanh thu theo Tháng")
# Tuỳ thuộc vào schema thực tế của bảng mart_revenue_analysis (ví dụ có cột month, total_revenue)
if 'month' in revenue_df.columns:
    st.bar_chart(revenue_df, x='month', y='total_revenue')
else:
    st.dataframe(revenue_df.head(10))

st.divider()

st.header("2. 🚦 Hiệu năng Giao thông (Traffic Efficiency)")
# Hiển thị bảng dữ liệu traffic (ví dụ điểm nóng kẹt xe, tốc độ TB theo giờ)
st.dataframe(traffic_df, use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.info("Đây là dự án Demo Data Engineering hoàn chỉnh sử dụng Spark Container và dbt.")
