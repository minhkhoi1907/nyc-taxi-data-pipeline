import streamlit as st
import pandas as pd
import altair as alt
from utils.db import execute_query

st.set_page_config(page_title="NYC Taxi Full BI Dashboard", layout="wide")

# Custom CSS for better KPI alignment
st.markdown("""
<style>
    [data-testid="stMetricValue"] { font-size: 1.8rem; color: #1f77b4; font-weight: bold; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem; color: #555; }
</style>
""", unsafe_allow_html=True)

st.title("🚖 NYC Taxi Business Intelligence Dashboard")

# Tabs
tab1, tab2, tab3 = st.tabs(["📈 Executive Overview", "🚗 Operational Logistics", "👥 Customer Segments"])

# --- TAB 1: EXECUTIVE OVERVIEW ---
with tab1:
    st.header("Tổng kết")
    
    # Pre-aggregated KPI query
    df_kpis = execute_query("""
        SELECT 
            SUM(total_revenue) as total_rev, 
            SUM(total_trips) as total_trips, 
            SUM(total_distance) as total_dist,
            SUM(total_tip) as total_tip
        FROM mart_revenue_analysis
    """)

    if df_kpis is not None and not df_kpis.empty:
        r = df_kpis.iloc[0]
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Tổng Doanh thu", f"${r.total_rev:,.0f}")
        k2.metric("Tổng Chuyến đi", f"{r.total_trips:,.0f}")
        
        rev_mile = r.total_rev/r.total_dist if r.total_dist > 0 else 0
        k3.metric("Doanh thu/Dặm", f"${rev_mile:.2f}")
        
        tip_pct = (r.total_tip/r.total_rev)*100 if r.total_rev > 0 else 0
        k4.metric("Tỷ lệ Tip TB", f"{tip_pct:.1f}%")

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Biểu đồ doanh thu theo tháng")
        st.caption("(Đơn vị: USD)")
        df_trend = execute_query("""
            SELECT 
                year || '-' || lpad(month::text, 2, '0') as "Tháng",
                SUM(total_revenue) as "Doanh thu"
            FROM mart_revenue_analysis
            WHERE year IS NOT NULL AND month IS NOT NULL 
              AND total_revenue > 0
              AND year::int >= 2022 -- Chỉ lấy từ năm 2022 như yêu cầu ban đầu
              AND (year::int < 2026 OR (year::int = 2026 AND month::int <= 2)) -- Chỉ lấy đến hết tháng 02/2026 (Tháng cuối cùng hoàn thiện)
            GROUP BY 1
            ORDER BY 1
        """)
        if df_trend is not None:
            line_chart = alt.Chart(df_trend).mark_line(color="#FF4B4B", point=True).encode(
                x=alt.X("Tháng:N", sort=None),
                y=alt.Y("Doanh thu:Q", axis=alt.Axis(format=",.0f")),
                tooltip=[alt.Tooltip("Tháng"), alt.Tooltip("Doanh thu", format=",.0f")]
            ).properties(height=400)
            st.altair_chart(line_chart, use_container_width=True)
    
    with c2:
        st.subheader("Top 10 Khu vực đón khách đông nhất")
        df_top_zones = execute_query("""
            SELECT zone as "Khu vực", SUM(total_trips) as "Số chuyến đi"
            FROM mart_trip_demand
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 10
        """)
        if df_top_zones is not None:
            bar_chart = alt.Chart(df_top_zones).mark_bar(color="#29b5e8").encode(
                x=alt.X("Khu vực:N", sort="-y"),
                y=alt.Y("Số chuyến đi:Q", axis=alt.Axis(format=",.0f")),
                tooltip=[alt.Tooltip("Khu vực"), alt.Tooltip("Số chuyến đi", format=",.0f")]
            ).properties(height=400)
            st.altair_chart(bar_chart, use_container_width=True)

# --- TAB 2: OPERATIONAL EFFICIENCY ---
with tab2:
    st.header("Hiệu quả Vận hành & Logistics")
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Tương quan Quãng đường vs Thời gian")
        df_scatter = execute_query("""
            SELECT 
                avg_distance as "Quãng đường (Miles)", 
                avg_duration_minutes as "Thời gian (Phút)", 
                distance_category as "Phân loại"
            FROM mart_traffic_efficiency
            WHERE avg_distance < 50 AND avg_duration_minutes < 200
            USING SAMPLE 5000
        """)
        if df_scatter is not None:
            scatter = alt.Chart(df_scatter).mark_circle(size=60).encode(
                x=alt.X("Quãng đường (Miles):Q"),
                y=alt.Y("Thời gian (Phút):Q", axis=alt.Axis(format=",.0f")),
                color="Phân loại:N",
                tooltip=[alt.Tooltip("Quãng đường (Miles)"), alt.Tooltip("Thời gian (Phút)", format=",.0f"), alt.Tooltip("Phân loại")]
            ).properties(height=400).interactive()
            st.altair_chart(scatter, use_container_width=True)
        
    with col_b:
        st.subheader("Tốc độ TB theo Phân loại Quãng đường")
        st.caption("(Đơn vị: MPH)")
        df_speed = execute_query("""
            SELECT 
                distance_category as "Phân loại", 
                AVG(avg_speed_mph) as "Tốc độ TB (MPH)"
            FROM mart_traffic_efficiency
            GROUP BY 1
        """)
        if df_speed is not None:
            speed_chart = alt.Chart(df_speed).mark_bar(color="#90be6d").encode(
                x=alt.X("Phân loại:N"),
                y=alt.Y("Tốc độ TB (MPH):Q", axis=alt.Axis(format=".1f")),
                tooltip=[alt.Tooltip("Phân loại"), alt.Tooltip("Tốc độ TB (MPH)", format=".1f")]
            ).properties(height=400)
            st.altair_chart(speed_chart, use_container_width=True)

# --- TAB 3: CUSTOMER BEHAVIOR ---
with tab3:
    st.header("Hành vi & Phân khúc Khách hàng")
    col_x, col_y = st.columns(2)
    
    with col_x:
        st.subheader("Phân tích Nhóm khách")
        df_pass = execute_query("""
            SELECT 
                passenger_group as "Nhóm khách", 
                SUM(total_trips) as "Số chuyến đi"
            FROM mart_customer_segmentation
            GROUP BY 1
        """)
        if df_pass is not None:
            pass_chart = alt.Chart(df_pass).mark_bar().encode(
                x=alt.X("Nhóm khách:N"),
                y=alt.Y("Số chuyến đi:Q", axis=alt.Axis(format=",.0f")),
                tooltip=[alt.Tooltip("Nhóm khách"), alt.Tooltip("Số chuyến đi", format=",.0f")]
            ).properties(height=400)
            st.altair_chart(pass_chart, use_container_width=True)
        
    with col_y:
        st.subheader("Nhu cầu đi Sân bay")
        df_airport = execute_query("""
            SELECT 
                is_airport as "Địa điểm", 
                SUM(total_trips) as "Số chuyến đi"
            FROM mart_customer_segmentation
            GROUP BY 1
        """)
        if df_airport is not None:
            df_airport["Nơi"] = df_airport["Nơi"].map({True: "Sân bay", False: "Nội thành"})
            airport_chart = alt.Chart(df_airport).mark_bar(color="#ffcdb2").encode(
                x=alt.X("Nơi:N"),
                y=alt.Y("Số chuyến đi:Q", axis=alt.Axis(format=",.0f")),
                tooltip=[alt.Tooltip("Nơi"), alt.Tooltip("Số chuyến đi", format=",.0f")]
            ).properties(height=400)
            st.altair_chart(airport_chart, use_container_width=True)