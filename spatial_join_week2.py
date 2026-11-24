"""Join mock GPS pings to 500-meter store catchments and save them to Snowflake."""

import os
import sys
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from sedona.spark import SedonaContext


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data-week2"
OUTPUT_TABLE = "GEOPULSE_DB.RAW.STORE_CATCHMENT_PINGS"
BATCH_SIZE = 5_000


def snowflake_options() -> dict[str, object]:
    load_dotenv(PROJECT_DIR / ".env")
    required = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Missing environment variable names in .env: " + ", ".join(missing)
        )

    options: dict[str, object] = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "GEOPULSE_WH"),
        "database": "GEOPULSE_DB",
        "schema": "RAW",
    }
    if os.getenv("SNOWFLAKE_ROLE"):
        options["role"] = os.environ["SNOWFLAKE_ROLE"]
    return options


def create_sedona_session() -> SparkSession:
    builder = (
        SedonaContext.builder()
        .master("local[*]")
        .appName("GeoPulseWeek2CatchmentJoin")
        .config(
            "spark.jars.packages",
            "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.7.1,"
            "org.datasyslab:geotools-wrapper:1.7.1-28.5",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )
    return SedonaContext.create(builder.getOrCreate())


def write_matches(
    matches: DataFrame, expected_count: int, options: dict[str, object]
) -> int:
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE} (
            STORE_ID VARCHAR NOT NULL,
            DEVICE_ID VARCHAR NOT NULL,
            PING_TIMESTAMP TIMESTAMP_NTZ NOT NULL
        )
    """
    insert_sql = (
        f"INSERT INTO {OUTPUT_TABLE} (STORE_ID, DEVICE_ID, PING_TIMESTAMP) "
        "VALUES (%s, %s, %s)"
    )

    # Replace only this Week 2 output table so reruns do not append duplicate rows.
    with snowflake.connector.connect(**options, autocommit=False) as connection:
        with connection.cursor() as cursor:
            cursor.execute(create_sql)
            cursor.execute(f"DELETE FROM {OUTPUT_TABLE}")

            batch = []
            inserted = 0
            for row in matches.toLocalIterator():
                batch.append((row.store_id, row.device_id, row.ping_timestamp))
                if len(batch) == BATCH_SIZE:
                    cursor.executemany(insert_sql, batch)
                    inserted += len(batch)
                    batch.clear()

            if batch:
                cursor.executemany(insert_sql, batch)
                inserted += len(batch)

            if inserted != expected_count:
                raise RuntimeError(
                    f"Expected to write {expected_count} rows, but streamed {inserted}"
                )
            connection.commit()
    return inserted


def main() -> None:
    pings_path = DATA_DIR / "pings.csv"
    stores_path = DATA_DIR / "stores.csv"
    for path in (pings_path, stores_path):
        if not path.is_file():
            raise FileNotFoundError(f"Required Week 2 input is missing: {path}")

    options = snowflake_options()
    spark = create_sedona_session()
    matches = None
    try:
        pings = spark.read.option("header", "true").csv(pings_path.as_uri())
        stores = spark.read.option("header", "true").csv(stores_path.as_uri())
        pings.createOrReplaceTempView("raw_pings")
        stores.createOrReplaceTempView("raw_stores")

        # Hyderabad is in UTM zone 44N. Projecting first makes the buffer distance meters.
        matches = spark.sql("""
            WITH ping_points AS (
                SELECT
                    device_id,
                    to_timestamp(ping_timestamp, 'yyyy-MM-dd HH:mm:ss') AS ping_timestamp,
                    ST_Transform(
                        ST_Point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE)),
                        'EPSG:4326', 'EPSG:32644'
                    ) AS point_geom
                FROM raw_pings
                WHERE device_id IS NOT NULL
                  AND latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  AND ping_timestamp IS NOT NULL
            ),
            store_buffers AS (
                SELECT
                    store_id,
                    ST_Buffer(
                        ST_Transform(
                            ST_Point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE)),
                            'EPSG:4326', 'EPSG:32644'
                        ),
                        500.0
                    ) AS catchment_geom
                FROM raw_stores
                WHERE store_id IS NOT NULL
                  AND latitude IS NOT NULL
                  AND longitude IS NOT NULL
            )
            SELECT /*+ BROADCAST(s) */
                s.store_id,
                p.device_id,
                p.ping_timestamp
            FROM ping_points p
            JOIN store_buffers s
              ON ST_Contains(s.catchment_geom, p.point_geom)
            WHERE p.ping_timestamp IS NOT NULL
        """).cache()

        match_count = matches.count()
        if match_count == 0:
            raise RuntimeError(
                "No pings matched the 500-meter store catchments; "
                "the Snowflake output table was not changed."
            )

        saved_count = write_matches(matches, match_count, options)
        print(
            f"Saved {saved_count:,} store-ping matches to {OUTPUT_TABLE}. "
            "Catchments use a 500-meter radius in EPSG:32644."
        )
    finally:
        if matches is not None:
            matches.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()