import streamlit as st
import duckdb
import os
import logging

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(ROOT_DIR, "nyc_taxi_dbt", "dev.duckdb")

@st.cache_resource
def get_connection():
    if not os.path.exists(DB_PATH):
        st.error(f"Database file not found at {DB_PATH}. Please execute the data pipeline first.")
        st.stop()
    return duckdb.connect(DB_PATH, read_only=True)

@st.cache_data(ttl=600)
def execute_query(query: str):
    try:
        conn = get_connection()
        return conn.execute(query).df()
    except Exception as e:
        st.error(f"Database query error: {e}")
        return None
