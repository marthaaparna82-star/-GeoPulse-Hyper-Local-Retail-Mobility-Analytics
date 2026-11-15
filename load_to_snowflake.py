#!/usr/bin/env python
"""Load generated CSVs into GEOPULSE_DB.RAW using Snowflake stages."""

import argparse
import os
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv


def load_csv(connection, path, stage_name, stage_table, target_table, columns, select_sql):
    database = "GEOPULSE_DB"
    schema = "RAW"
    file_uri = "file://" + quote(path.resolve().as_posix(), safe="/:")
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TEMPORARY STAGE {stage_name}")
        cursor.execute(f"CREATE TEMPORARY TABLE {stage_table} ({columns})")
        cursor.execute(
            f"PUT {file_uri} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )
        cursor.execute(
            f"COPY INTO {stage_table} FROM @{stage_name} "
            "FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"') "
            "ON_ERROR='ABORT_STATEMENT' PURGE=TRUE"
        )
        cursor.execute(f"SELECT COUNT(*) FROM {stage_table}")
        row_count = cursor.fetchone()[0]
        cursor.execute(
            f"INSERT INTO {database}.{schema}.{target_table} "
            f"SELECT {select_sql} FROM {stage_table}"
        )
    print(f"Loaded {row_count:,} rows from {path}")
    return row_count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument(
        "--replace",
        action="store_true",
        help="truncate target tables before loading instead of appending",
    )
    args = parser.parse_args()
    pings_path = args.data_dir / "pings.csv"
    stores_path = args.data_dir / "stores.csv"
    for path in (pings_path, stores_path):
        if not path.is_file():
            parser.error(f"Required CSV not found: {path}")

    load_dotenv()
    required = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        parser.error(f"Missing environment variables: {', '.join(missing)}")

    connection_options = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "GEOPULSE_WH"),
        "database": "GEOPULSE_DB",
        "schema": "RAW",
    }
    if os.getenv("SNOWFLAKE_ROLE"):
        connection_options["role"] = os.environ["SNOWFLAKE_ROLE"]

    import snowflake.connector

    with snowflake.connector.connect(**connection_options) as connection:
        if args.replace:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE GEOPULSE_DB.RAW.DEVICE_PINGS")
                cursor.execute("TRUNCATE TABLE GEOPULSE_DB.RAW.STORES")

        ping_rows = load_csv(
            connection,
            pings_path,
            "PINGS_FILE_STAGE",
            "PINGS_LOAD_STAGE",
            "DEVICE_PINGS",
            "DEVICE_ID VARCHAR, LATITUDE FLOAT, LONGITUDE FLOAT, "
            "PING_TIMESTAMP VARCHAR",
            "DEVICE_ID, ST_MAKEPOINT(LONGITUDE, LATITUDE), "
            "TO_TIMESTAMP_NTZ(PING_TIMESTAMP)",
        )
        store_rows = load_csv(
            connection,
            stores_path,
            "STORES_FILE_STAGE",
            "STORES_LOAD_STAGE",
            "STORES",
            "STORE_ID VARCHAR, NAME VARCHAR, LATITUDE FLOAT, LONGITUDE FLOAT",
            "STORE_ID, NAME, ST_MAKEPOINT(LONGITUDE, LATITUDE)",
        )
    print(f"Done: {ping_rows:,} pings and {store_rows:,} stores")


if __name__ == "__main__":
    main()
