"""
Module for loading cryptocurrency assets data and implementing SCD2 logic in DuckDB.
"""

import os
from datetime import datetime
import pandas as pd
import duckdb
from .logger_config import setup_logger
from .extractor import CoinCapExtractor

# This logger will be used to log messages in the script
logger = setup_logger(__name__)


def load_assets():
    """
    Load assets from CoinCap API and insert them into dim_assets table in DuckDB.
    """

    extractor = CoinCapExtractor(endpoint="v3/assets")

    data = extractor.get_data()
    assets = data[0]["data"]
    df = pd.DataFrame(assets)

    df_filtered = df[["id", "name", "rank", "symbol"]].copy()
    df_filtered.loc[:, "start_date"] = datetime.now()

    db_path = os.path.join("data", "CoinCap.db")
    conn = duckdb.connect(db_path)
    try:
        conn.sql(
            """
            INSERT INTO dim_assets BY Name
            SELECT
                id as asset_id,
                name,
                rank,
                symbol,
                current_timestamp as load_timestamp,
                start_date,
                '9999-12-31' as end_date,
                TRUE as is_current
                FROM df_filtered
                """
        )
        print("Data succesfully loaded into dim_assets")
    except Exception as e:
        print(f"Error during load data into dim_assets: {e}")
    finally:
        conn.close()


#   #   #   #   #
#  TO revision SCD2
#   test data
#   #   #   #   #


def scd2():
    """
    Implement Slowly Changing Dimension Type 2 (SCD2) logic to update dim_assets table.
    """
    # name changed
    new_data = pd.DataFrame(
        [
            {"asset_id": "bitcoin", "name": "Bitcoin", "symbol": "BTC", "rank": 1},
            {"asset_id": "ethereum", "name": "Ethere", "symbol": "ETH", "rank": 2},
        ]
    )

    new_data = pd.DataFrame(new_data)
    new_data["start_date"] = datetime.now
    new_data["load_timestamp"] = datetime.now
    new_data["end_date"] = "9999-12-31"
    new_data["is_current"] = True

    db_path = os.path.join("data", "CoinCap.db")
    conn = duckdb.connect(db_path)
    existing = conn.sql("SELECT * FROM dim_assets WHERE is_current = TRUE").fetchdf()
    for _, new_row in new_data.iterrows():
        asset_id = new_row["asset_id"]

        # Find current record for this asset
        current = existing["asset_id"] == asset_id

        # If no existing record, just insert
        if current.empty:
            conn.sql(
                "INSERT INTO dim_assets SELECT * FROM new_data WHERE asset_id=?",
                [asset_id],
            )
        else:
            # Check if name or symbol changed
            curr_row = current.iloc[0]
            if curr_row["name"] != new_row["name"] or curr_row["symbol"] != new_row["symbol"]:
                # Update old record
                conn.sql(
                    """
                         UPDATE dim_assets
                         SET end_date = ? AND is_current = TRUE
                        """,
                    [datetime.now(), asset_id],
                )
                # Insert new version
                conn.sql(
                    "INSERT INTO dim_assets SELECT * FROM new_data WHERE asset_id = ?",
                    [asset_id],
                )
