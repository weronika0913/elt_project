import streamlit as st
import duckdb
import pandas as pd
import os
import altair as alt 

# ---- UI DASHBOARDU ----
st.set_page_config(page_title="CoinCap Analysis", 
                   layout="centered")
st.title("CoinCap Analysis")
duckdb_path = "/app/data/CoinCap.db"

if not os.path.exists(duckdb_path):
    st.info("Waiting for input data")
else:
   conn = duckdb.connect(duckdb_path)


if st.button("🔄 Refresh data"):
    st.cache_data.clear()
    st.rerun()

@st.cache_data
def load_history_price():
    df = conn.execute("""
    SELECT *
    FROM history_price
    """).fetchdf()
    conn.close()
    return df

df = load_history_price()

unique_asset = df["asset_id"].drop_duplicates()

# ---- SIDE BAR ---
st.sidebar.header("📌 Please Filter Here:")
start_date = st.sidebar.date_input("Select Start Date", df["date_id"].min())
end_date = st.sidebar.date_input("Select End Date", df["date_id"].max())
asset = st.sidebar.selectbox("Select asset", unique_asset)


# ----- LINE CHART for price -----
st.write("Price Change of Selected Asset Over Time")
df_filtered = df[(df["date_id"].dt.date >= start_date) & (df["date_id"].dt.date <= end_date) & (df["asset_id"] == asset)]



# Calculate Max and Min prices
max_price = df_filtered["price_usd"].max()
min_price = df_filtered["price_usd"].min()

# Use columns to display the cards side by side
col1, col2 = st.columns(2)

# Max Price - wheel card
col1.markdown(
    f"""
    <div style="background-color: #81C784; color: white; width: 160px; height: 160px; 
                border-radius: 50%; display: flex; flex-direction: column; 
                align-items: center; justify-content: center; 
                box-shadow: 0 6px 12px rgba(0, 0, 0, 0.25); 
                font-size: 16px; text-align: center; margin: auto;">
        <b>Max</b>
        <div style="font-size: 22px; font-weight: bold;">${max_price:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

# Min Price - wheel card
col2.markdown(
    f"""
    <div style="background-color: #FF8A65; color: white; width: 160px; height: 160px; 
                border-radius: 50%; display: flex; flex-direction: column; 
                align-items: center; justify-content: center; 
                box-shadow: 0 6px 12px rgba(0, 0, 0, 0.25); 
                font-size: 16px; text-align: center; margin: auto;">
        <b>Min</b>
        <div style="font-size: 22px; font-weight: bold;">${min_price:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)


st.write("")
st.line_chart(df_filtered.set_index("date_id")["price_usd"])