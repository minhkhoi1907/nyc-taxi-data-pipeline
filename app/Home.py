import streamlit as st

st.set_page_config(
    page_title="NYC Taxi Pipeline Analytics",
    layout="wide"
)

st.title("NYC Taxi Data Pipeline: Medallion Architecture")

st.markdown("""
### End-to-End Orchestration Workflow

This project demonstrates a production-grade data engineering pipeline covering data extraction, processing, warehousing, and visualization.

1. **Extraction (Web to HDFS):** Historical TLC `.parquet` datasets are dynamically fetched via REST API and persisted in an HDFS Data Lake raw tier.
2. **Batch Processing (PySpark):** 
   - Distributed processing using an Apache Spark cluster.
   - Core transformations include schema standardization parsing legacy data structures into modern standard formats.
   - Cleansed data is partitioned and written back to the HDFS processed tier.
3. **Data Warehousing (DuckDB & dbt):**
   - Optimized columnar storage engines via DuckDB.
   - Modular SQL transformations governed by dbt (data build tool).
4. **Presentation (Streamlit):** Web-based reporting module visualizing BI marts.

**Navigation**
Select a specific data mart from the sidebar to explore metrics such as revenue streams, network traffic, customer demographics, and demand fluctuations.
""")
