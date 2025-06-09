"""
Module for initializing the data warehouse for the CoinCap project.

This module contains functions to create and prepare the necessary tables
and load essential dimension data for the data warehouse.

Functions include:
- create_tables(): Creates or replaces staging, dimension, and fact tables in DuckDB.
- insert_into_date_table(): Generates and loads the date dimension data covering 2024-2030.
"""

import os
import duckdb
import pandas as pd
from .logger_config import setup_logger

logger = setup_logger(__name__)


def create_tables():
    """
    Creates or replaces the main tables in the DuckDB database for the CoinCap project.

    The tables created are:
    - staging_history_price: temporary table for raw price data.
    - dim_assets: dimension table for assets with SCD2 structure.
    - dim_date: date dimension table with hourly granularity.
    - history_price: fact table for historical prices.
    - fact_coin_metrics: fact table for additional coin metrics.

    The tables have primary keys and default values set where appropriate.
    """
    db_path = os.path.join("data", "CoinCap.db")
    conn = duckdb.connect(db_path)
    try:
        conn.sql(
            """
          CREATE OR REPLACE TABLE staging_history_price(
                    asset_id text,
                    priceUsd decimal(18,2),
                    time bigint,
                    date timestamp,
                    load_timestamp timestamp DEFAULT CURRENT_TIMESTAMP
                   );

          CREATE OR REPLACE TABLE dim_assets (
                    asset_id text,
                    name text,
                    rank int,
                    symbol text,
                    load_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
                    start_date timestamp,
                    end_date timestamp DEFAULT '9999-12-31',
                    is_current boolean DEFAULT TRUE,
                    PRIMARY KEY(asset_id, start_date)
                    );
          CREATE OR REPLACE TABLE dim_date (
               date_id timestamp PRIMARY KEY,
               year int,
               month int,
               day int,
               hour int
               );
          CREATE OR REPLACE TABLE history_price (
               asset_id text,
               date_id timestamp,
               price_usd decimal(18,2),
               load_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
               PRIMARY KEY(asset_id, date_id)
               );
          CREATE OR REPLACE TABLE fact_coin_metrics (
               metric_id UUID,
               asset_id text,
               date_id date,
               supply decimal(18,2),
               maxSupply decimal(18,2),
               marketCapUsd decimal (18,2),
               priceUsd decimal (18,2),
               load_timestamp timestamp
               )
          """
        )
        logger.info("Tables dim_assets, dim_date, history_price has been successfully created")
    except Exception as e:
        logger.error("Error during creating dim_assets, dim_date, history_price: %s", e)
        raise
    finally:
        conn.close()


def insert_into_date_table():
    """
    Generates and inserts hourly date records into the dim_date table.

    The date range covers from January 1, 2024, to December 31, 2030.
    For each hour in this period, the table stores date_id (timestamp), year, month, day, and hour.

    This table is used as a date dimension for joining with fact tables.
    """
    db_path = os.path.join("data", "CoinCap.db")
    conn = duckdb.connect(db_path)

    date_range = pd.date_range("2024-01-01", "2030-12-31", freq="h")

    generate_dim_date = pd.DataFrame(
        {
            "date_id": date_range,  # pylint: disable=no-member
            "year": date_range.year,  # pylint: disable=no-member
            "month": date_range.month,  # pylint: disable=no-member
            "day": date_range.day,  # pylint: disable=no-member
            "hour": date_range.hour,  # pylint: disable=no-member
        }
    )
    conn.register("generate_dim_date", generate_dim_date)
    try:
        conn.sql(
            """
               INSERT INTO dim_date BY NAME
               SELECT * from generate_dim_date
            """
        )
        logger.info("Data has been succesfully loaded into dim_date")
    except Exception as e:
        logger.error("Error inserting into dim_date: %s", e)
        raise
    finally:
        conn.close()
