import streamlit as st
import pandas as pd
import json
import os

st.set_page_config(layout="wide")

BASE_PATH = "."

# -------------------------------
# HELPERS
# -------------------------------
def load_csv(path):
    try:
        if os.path.exists(path):
            return pd.read_csv(path)
        else:
            st.warning(f"Missing file: {path}")
            return None
    except Exception as e:
        st.warning(f"Error loading {path}: {e}")
        return None


def load_json(path):
    try:
        if os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
        else:
            st.warning(f"Missing file: {path}")
            return None
    except Exception as e:
        st.warning(f"Error loading {path}: {e}")
        return None


def safe_line_chart(df):
    try:
        if df is not None and not df.empty:
            numeric_cols = df.select_dtypes(include=["number"]).columns
            if len(numeric_cols) >= 1:
                st.line_chart(df[numeric_cols])
    except:
        pass


def safe_bar_chart(df):
    try:
        if df is not None and not df.empty:
            numeric_cols = df.select_dtypes(include=["number"]).columns
            if len(numeric_cols) >= 1:
                st.bar_chart(df[numeric_cols])
    except:
        pass


# -------------------------------
# TITLE + SIDEBAR
# -------------------------------
st.title("Smart Meter MDMS Dashboard")

st.sidebar.header("Project Info")
st.sidebar.write("""
End-to-End Smart Meter Data Engineering Pipeline

This project presents a complete data engineering pipeline for smart meter data, covering ingestion, processing, analytics, and visualization. It simulates real-world smart grid systems using scalable architecture.

The pipeline includes data cleaning, feature engineering, validation, storage (CSV, Parquet, SQLite), SQL analytics, and visualization. Insights such as consumption trends, peak loads, anomalies, and zone-wise performance are generated.

A modular Python pipeline ensures automation and reproducibility, while a Streamlit dashboard enables interactive analysis of energy data.
""")

# -------------------------------
# TABS
# -------------------------------
tabs = st.tabs([
    "Overview",
    "Trends",
    "Zone Analytics",
    "Load Analysis",
    "Anomaly Detection",
    "Visual Dashboard"
])

# =====================================================
# TAB 1: OVERVIEW
# =====================================================
with tabs[0]:

    cleaned = load_csv(os.path.join(BASE_PATH, "data/processed/cleaned_data.csv"))
    featured = load_csv(os.path.join(BASE_PATH, "data/curated/featured_data.csv"))
    sql_summary = load_json(os.path.join(BASE_PATH, "outputs/reports/sql_summary.json"))
    exec_summary = load_json(os.path.join(BASE_PATH, "outputs/reports/executive_summary.json"))

    col1, col2, col3, col4 = st.columns(4)

    total_records = len(cleaned) if cleaned is not None else 0
    avg_consumption = cleaned.select_dtypes(include="number").mean().mean() if cleaned is not None else 0
    zones = cleaned["Zone_ID"].nunique() if cleaned is not None and "Zone_ID" in cleaned.columns else 0
    total_csv = len(sql_summary["generated_csv_files"]) if sql_summary else 0

    col1.metric("Total Records", total_records)
    col2.metric("Avg Consumption", round(avg_consumption, 2))
    col3.metric("Zones", zones)
    col4.metric("CSV Outputs", total_csv)

    if exec_summary:
        st.subheader("Executive Summary")
        st.json(exec_summary)

    if sql_summary:
        st.subheader("SQL Summary")
        st.json(sql_summary)

    if cleaned is not None:
        st.subheader("Cleaned Data Preview")
        st.dataframe(cleaned.head())

    if featured is not None:
        st.subheader("Featured Data Preview")
        st.dataframe(featured.head())


# =====================================================
# TAB 2: TRENDS (FIXED)
# =====================================================
with tabs[1]:

    # ✅ ORDER FIXED (daily first)
    files = [
        "daily_consumption.csv",
        "hourly_consumption.csv",
        "moving_avg_24h.csv",
        "rolling_avg.csv",
        "cumulative_consumption.csv",
        "quarterly_growth.csv"
    ]

    for file in files:
        st.subheader(file)

        df = load_csv(os.path.join(BASE_PATH, "data/analytics", file))

        if df is not None:
            # Chart
            safe_line_chart(df)

            # ✅ TABLE ADDED
            st.dataframe(df.head(20))  # show top rows (clean + fast)


# =====================================================
# TAB 3: ZONE ANALYTICS
# =====================================================
with tabs[2]:

    files = [
        "zone_analysis.csv",
        "zone_statistics.csv",
        "top_consumers.csv",
        "efficiency_scores.csv"
    ]

    for file in files:
        st.subheader(file)
        df = load_csv(os.path.join(BASE_PATH, "data/analytics", file))
        if df is not None:
            safe_bar_chart(df)
            st.dataframe(df)


# =====================================================
# TAB 4: LOAD ANALYSIS
# =====================================================
with tabs[3]:

    files = [
        "peak_hours.csv",
        "power_factor_analysis.csv",
        "load_volatility.csv"
    ]

    for file in files:
        st.subheader(file)
        df = load_csv(os.path.join(BASE_PATH, "data/analytics", file))
        if df is not None:
            safe_bar_chart(df)
            st.dataframe(df)


# =====================================================
# TAB 5: ANOMALY DETECTION
# =====================================================
with tabs[4]:

    files = [
        "anomaly_detection.csv",
        "consumption_spikes.csv"
    ]

    for file in files:
        st.subheader(file)
        df = load_csv(os.path.join(BASE_PATH, "data/analytics", file))
        if df is not None:
            safe_bar_chart(df)
            st.dataframe(df)


# =====================================================
# TAB 6: VISUAL DASHBOARD
# =====================================================
with tabs[5]:

    plot_files = [
        "01_hourly_consumption.png",
        "02_daily_consumption.png",
        "03_monthly_consumption.png",
        "04_zone_comparison.png",
        "05_peak_analysis.png",
        "09_reactive_power.png",
        "06_power_distribution.png",
        "08_top_consumers.png",
        "07_anomaly_analysis.png",
        "10_voltage_analysis.png"
    ]

    for i in range(0, len(plot_files), 2):
        col1, col2 = st.columns(2)

        path1 = os.path.join(BASE_PATH, "outputs/plots", plot_files[i])
        path2 = os.path.join(BASE_PATH, "outputs/plots", plot_files[i+1])

        if os.path.exists(path1):
            col1.image(path1, use_container_width=True)
        else:
            col1.warning(f"Missing: {plot_files[i]}")

        if os.path.exists(path2):
            col2.image(path2, use_container_width=True)
        else:
            col2.warning(f"Missing: {plot_files[i+1]}")