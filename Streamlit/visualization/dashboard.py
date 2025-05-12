import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os

# ---- UI DASHBOARDU ----
st.set_page_config(page_title="CoinCap Analysis", layout="wide")

#Connection to db
duckdb_path = "/opt/airflow/data/CoinCap.db"

if not os.path.exists(duckdb_path):
    st.info("Waiting for input data")
else:
   conn = duckdb.connect(duckdb_path)


# #fetch data
# @st.cache_data # decorator for caching data
# def load_data_from_db():
#     return conn.execute("SELECT priceUSD, circulatingSupply, date FROM bitcoin").fetchdf()

# if st.button("🔄 Refresh data"):
#     st.cache_data.clear()
#     st.rerun()

# df = load_data_from_db()
# df_transformed = df.rename(columns={"priceUsd": "Price (USD)", "date": "Date", "circulatingSupply": "Circulating Supply (BTC)"})

# # -- HEADER --
# st.title("📈 Bitcoin Price Dashboard")
# st.write("10 most recent historical data of Bitcoin prices")

# # ---- TABLE ----
# st.subheader("🔍 Data Preview")
# first_10_rows = df_transformed.sort_values(by="Date", ascending=False).head(10)
# st.dataframe(first_10_rows)

# # ---- SIDE BAR ----
# st.sidebar.header("📌 Please Filter Here:")
# start_date = st.sidebar.date_input("Select Start Date", df["date"].min())
# end_date = st.sidebar.date_input("Select End Date", df["date"].max())

# df_filtered = df_transformed[(df_transformed["Date"] >= pd.Timestamp(start_date)) & (df_transformed["Date"] <= pd.Timestamp(end_date))]

# # ---- LINE CHART PRICE ----
# st.subheader("📊 Bitcoin price chart")
# fig = px.line(df_filtered,
#                 x="Date",
#                 y="Price (USD)",
#                 title="Bitcoin price over time",
#                 )
# st.plotly_chart(fig)

# # ---- LINE CHART CIRCULATING ----
# st.subheader("📊 Circulating Supply of Bitcoin")
# fig = px.line(df_filtered,
#                 x="Date",
#                 y="Circulating Supply (BTC)",
#                 title="Bitcoin Circulating Supply Over Time")
# st.plotly_chart(fig)


# # ---- SCATTER PLOT OF PRICE VS VOLUME ----
# st.subheader("📊 Bitcoin Price vs Circulating Supply")

# fig = px.scatter(df_filtered, 
#                  x="Circulating Supply (BTC)", 
#                  y="Price (USD)",
#                  title="Bitcoin Price vs Circulating Supply"
#                  )
# st.plotly_chart(fig)

# conn.close()