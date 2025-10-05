# app.py
import streamlit as st
import pandas as pd
import sqlite3
import os
import plotly.express as px

# Import ETL run function
from etl import run_etl, DB_NAME, TABLE_NAME, LOG_PATH

st.set_page_config(page_title="Crypto ETL Dashboard", layout="wide")
st.title("📊 Crypto ETL Dashboard (PKR conversions)")

col1, col2 = st.columns([1, 3])

with col1:
    st.header("Controls")
    if st.button("Run ETL now"):
        df = run_etl()
        if df is not None and not df.empty:
            st.success("ETL run completed")
            st.write(f"Rows: {len(df)}")
        else:
            st.error("ETL run failed or returned no data!")

    if st.button("Refresh from DB"):
        pass  # refresh handled below

    show_logs = st.checkbox("Show logs")

with col2:
    # Load from DB if available
    def load_from_db(db=DB_NAME, table=TABLE_NAME):
        if not os.path.exists(db):
            st.warning(f"Database '{db}' not found. Run ETL first.")
            return pd.DataFrame()
        try:
            conn = sqlite3.connect(db)
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Failed to load data from DB: {e}")
            return pd.DataFrame()

    df = load_from_db()

    if df.empty:
        st.info("No data available. Click 'Run ETL now' to fetch data.")
    else:
        st.subheader("Data Table")
        st.dataframe(df, use_container_width=True)

        # pick chart column — choose price_in_ columns first, else present numeric columns
        price_cols = [c for c in df.columns if c.startswith("price_in_")]
        numeric_cols = df.select_dtypes(include='number').columns.tolist()

        chart_col = st.selectbox("Select column to visualize", options=(price_cols if price_cols else numeric_cols))

        if chart_col:
            fig = px.bar(df.sort_values(chart_col, ascending=False), x="crypto", y=chart_col, text=chart_col)
            st.plotly_chart(fig, use_container_width=True)

        # show timestamp
        if "last_updated" in df.columns:
            st.write("Last updated (first row):", df["last_updated"].iloc[0])

    if show_logs:
        if os.path.exists(LOG_PATH):
            with open(LOG_PATH, "r") as f:
                logs = f.read()
            st.subheader("ETL Logs")
            st.text_area("logs", logs, height=500)
        else:
            st.info("No logs found yet.")

st.markdown("---")
st.markdown("Tips: Use the `Run ETL now` button to fetch live rates and update SQLite DB. Then use `Refresh from DB` to view.")
