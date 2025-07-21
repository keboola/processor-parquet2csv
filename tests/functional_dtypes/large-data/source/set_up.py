import logging
import os

import duckdb
from datadirtest import TestDataDir


def run(context: TestDataDir):
    # 50_000_000 mil rows is approximately 1,5 GB parquet file with Snappy compression
    con = duckdb.connect(database=":memory:", read_only=False)
    con.execute(f"""

    CREATE TABLE dummy_table AS (
    SELECT
        r.range AS "row_number",
        FLOOR(RANDOM() * 899999)::INT AS "order_id",
        DATE '2023-01-01' + INTERVAL (FLOOR(RANDOM() * 365)) DAY AS "order_date",
        ROUND(CAST((RANDOM() * 990.0 + 10.0) AS DECIMAL(10, 2)), 2) AS "order_amount",
        RANDOM() * 100 AS discount,
        RANDOM() > 0.5 AS is_active,
        CAST(FLOOR(RANDOM() * 256) AS DOUBLE) AS "double",
        RANDOM() * 1000 AS large_number,
        'How to become data driven startup' as "text"
    FROM range(50_000_000) AS r);

    COPY (SELECT * FROM dummy_table)
    TO '{context.source_data_dir}/in/files/table.parquet' (FORMAT PARQUET);

    COPY (SELECT *, '/table.parquet' as parquet_filename FROM dummy_table)
    TO '{context.data_dir}/expected/data/out/tables/table.csv' (HEADER FALSE);

    """)

    size = os.path.getsize(f"{context.source_data_dir}/in/files/table.parquet")

    logging.info(f"Created dummy table with size: {size / (1024 * 1024):.2f} MB")

    con.close()
