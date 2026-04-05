import urllib.request
import json
import math
import streamlit as st
import pandas as pd
import altair as alt
import pydeck as pdk
from utils.db import execute_query

st.set_page_config(page_title="NYC Taxi Full BI Dashboard", layout="wide")

# Custom CSS cho các thẻ KPI
st.markdown("""
<style>
    [data-testid="stMetricValue"] { font-size: 1.8rem; color: #1f77b4; font-weight: bold; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem; color: #555; }
</style>
""", unsafe_allow_html=True)

st.title("🚖 NYC Taxi Business Intelligence Dashboard")

# --- SECTION 1: EXECUTIVE OVERVIEW ---
st.header(" Tổng kết")

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
    
    rev_km = r.total_rev / (r.total_dist * 1.609) if r.total_dist > 0 else 0
    k3.metric("Doanh thu/KM", f"${rev_km:.2f}")
    
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
          AND year::int >= 2022 
          AND (year::int < 2026 OR (year::int = 2026 AND month::int <= 2)) 
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

st.divider()

# Legend will be moved inside tabs


@st.cache_data
def get_geojson_with_data(df_trips, mode="trips"):
    import os
    file_path = os.path.join(os.path.dirname(__file__), "taxi_zones.geojson")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            geojson = json.load(f)
    except:
        return None
        
    if mode == "trips":
        data_map = dict(zip(df_trips["location_id"].astype(str), df_trips["total_trips"]))
        label = "Số chuyến"
    else:
        # Tip Percentage mode
        data_map = dict(zip(df_trips["location_id"].astype(str), df_trips["tip_percentage"] * 100))
        label = "Tỷ lệ Tip (%)"

    all_vals = sorted([v for v in data_map.values() if v > 0])
    if not all_vals:
        p40, p80 = (1000, 50000) if mode == "trips" else (5, 15)
    else:
        p40 = all_vals[int(len(all_vals) * 0.40)]
        p80 = all_vals[int(len(all_vals) * 0.80)]
    
    max_val = max(all_vals) if all_vals else 1
    
    for feature in geojson['features']:
        loc_id = str(feature['properties'].get('LocationID', '0'))
        val = data_map.get(loc_id, 0)
        
        feature['properties']['value'] = round(val, 1) if mode == "tips" else int(val)
        feature['properties']['formatted_val'] = f"{feature['properties']['value']:,}" + ("%" if mode == "tips" else "")
        
        if val > 0:
            if val <= p40:
                sub_t = min(val / p40, 1.0)
                r, g, b = int(200 - sub_t * 160), int(240 - sub_t * 80), int(200 - sub_t * 160)
            elif val <= p80:
                sub_t = min((val - p40) / (p80 - p40), 1.0)
                r, g, b = int(255 - sub_t * 15), int(240 - sub_t * 110), int(150 - sub_t * 150)
            else:
                log_v = math.log1p(val)
                log_p80 = math.log1p(p80)
                log_max = math.log1p(max_val)
                intensity = (log_v - log_p80) / (log_max - log_p80) if log_max > log_p80 else 1.0
                sub_t = min(intensity, 1.0)
                r, g, b = int(230 - sub_t * 100), int(80 - sub_t * 80), int(80 - sub_t * 80)
            fill = [r, g, b, 175] 
        else:
            fill = [200, 200, 200, 45]
            
        feature['properties']['fill_color'] = fill
        
    return geojson, label

st.header("Mật độ Chuyến đi và Tỷ lệ Tip")

tab_map1, tab_map2 = st.tabs(["Mật độ Chuyến đi", "Tỷ lệ Tip trung bình"])

df_zones_all = execute_query(f"""
    SELECT 
        pickup_location_id as location_id, 
        SUM(total_trips) as total_trips,
        AVG(tip_percentage) as tip_percentage
    FROM mart_revenue_analysis
    GROUP BY 1
""")

def display_map(df, mode):
    if df is not None and not df.empty:
        with st.spinner(f"Đang xử lý bản đồ {mode}..."):
            geojson_data, val_label = get_geojson_with_data(df, mode=mode)
            if geojson_data:
                # Legend chuyên biệt cho từng mode
                label_text = "Mật độ khách:" if mode == "trips" else "Mức độ hào phóng:"
                low_label = "Vắng khách" if mode == "trips" else "Tip thấp"
                high_label = "Đông khách" if mode == "trips" else "Tip cao"
                
                st.markdown(f"""<div style="display: flex; align-items: center; margin-bottom: 20px; padding: 10px; background-color: #fcfcfc; border-radius: 8px; border: 1px solid #eee;">
                <span style="font-weight: bold; margin-right: 15px; color: #333;">{label_text}</span>
                <div style="background: linear-gradient(to right, rgb(200,240,200), rgb(40,160,40)); width: 50px; height: 15px; border-radius: 3px; margin-right: 5px;"></div>
                <span style="margin-right: 20px; color: #666; font-size: 0.85em;">{low_label}</span>
                <div style="background: linear-gradient(to right, rgb(255,240,150), rgb(240,130,0)); width: 50px; height: 15px; border-radius: 3px; margin-right: 5px;"></div>
                <span style="margin-right: 20px; color: #666; font-size: 0.85em;">Trung bình</span>
                <div style="background: linear-gradient(to right, rgb(230,80,80), rgb(130,0,0)); width: 50px; height: 15px; border-radius: 3px; margin-right: 5px;"></div>
                <span style="color: #666; font-size: 0.85em;">{high_label}</span>
                </div>""", unsafe_allow_html=True)

                layer = pdk.Layer(
                    "GeoJsonLayer",
                    geojson_data,
                    pickable=True,
                    stroked=True,
                    filled=True,
                    get_fill_color="properties.fill_color",
                    get_line_color=[255, 255, 255, 180], 
                    line_width_min_pixels=1
                )
                
                view_state = pdk.ViewState(
                    longitude=-73.96,
                    latitude=40.75,
                    zoom=10, 
                    pitch=0,
                )
                
                r = pdk.Deck(
                    layers=[layer], 
                    initial_view_state=view_state, 
                    map_style="road", 
                    tooltip={
                        "html": f"<b>Khu vực:</b> {{zone}}<br/><b>{val_label}:</b> {{formatted_val}}",
                        "style": {"backgroundColor": "#2C3E50", "color": "white", "font-family": "sans-serif"}
                    }
                )
                st.pydeck_chart(r)
            else:
                st.error("Không tìm thấy file taxi_zones.geojson")

with tab_map1:
    display_map(df_zones_all, mode="trips")

with tab_map2:
    display_map(df_zones_all, mode="tips")


st.divider()
# --- SECTION 2: CUSTOMER BEHAVIOR ---
st.header("Nhu cầu khách hàng")
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
            is_airport as "Nơi", 
            SUM(total_trips) as "Số chuyến đi"
        FROM mart_customer_segmentation
        GROUP BY 1
    """)
    if df_airport is not None and not df_airport.empty:
        df_plot = df_airport.copy()
        df_plot["Nơi"] = df_plot["Nơi"].replace({True: "Sân bay", False: "Nội thành"})
        
        airport_chart = alt.Chart(df_plot).mark_bar(color="#ffcdb2").encode(
            x=alt.X(
                "Nơi:N", 
                title="Địa điểm", 
                axis=alt.Axis(labelAngle=0)
            ),
            y=alt.Y(
                "Số chuyến đi:Q", 
                title="Số lượng chuyến", 
                axis=alt.Axis(format=",.0f")
            ),
            tooltip=[
                alt.Tooltip("Nơi:N", title="Địa điểm"), 
                alt.Tooltip("Số chuyến đi:Q", title="Số chuyến đi", format=",.0f")
            ]
        ).properties(
            height=400,
            title="Thống kê số chuyến đi theo địa điểm"
        )
        
        st.altair_chart(airport_chart, use_container_width=True)

st.divider()

# --- SECTION 3: STRATEGIC INSIGHTS (WOW FACTOR) ---
st.header("Doanh thu trung bình theo thời gian và nhà cung cấp")
tab_inv1, tab_inv2 = st.columns(2)

with tab_inv1:
    st.caption("Doanh thu trung bình theo giờ")
    df_golden = execute_query("""
        SELECT 
            hour as "Giờ",
            SUM(total_revenue) / SUM(total_trips) as "Doanh thu/Chuyến"
        FROM mart_revenue_analysis
        GROUP BY 1
        ORDER BY 1
    """)
    if df_golden is not None:
        golden_chart = alt.Chart(df_golden).mark_area(
            line={'color':'#ff9f1c'},
            color=alt.Gradient(
                gradient='linear',
                stops=[alt.GradientStop(color='#ff9f1c', offset=0),
                      alt.GradientStop(color='white', offset=1)],
                x1=1, x2=1, y1=1, y2=0
            )
        ).encode(
            x=alt.X("Giờ:O"),
            y=alt.Y("Doanh thu/Chuyến:Q", title="USD per Trip"),
            tooltip=["Giờ", alt.Tooltip("Doanh thu/Chuyến", format=".2f")]
        ).properties(height=350)
        st.altair_chart(golden_chart, use_container_width=True)
with tab_inv2:
    st.subheader(" Thị phần nhà cung cấp")
    df_vendor = execute_query("""
        SELECT 
            CASE WHEN vendor_id = 1 THEN 'Creative Mobile' ELSE 'Verifone' END as "Hãng",
            SUM(total_trips) as "Chuyến đi"
        FROM mart_revenue_analysis
        GROUP BY 1
    """)
    if df_vendor is not None:
        pie = alt.Chart(df_vendor).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="Chuyến đi", type="quantitative"),
            color=alt.Color(field="Hãng", type="nominal", scale=alt.Scale(range=['#1f77b4', '#ff7f0e'])),
            tooltip=["Hãng", alt.Tooltip("Chuyến đi", format=",")]
        ).properties(height=350)
        st.altair_chart(pie, use_container_width=True)