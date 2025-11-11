import pandas as pd
import os
import io
import boto3
import sys
import psycopg2
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_utils import get_connection, prepare_dblink, upload_to_s3,get_batch_date_from_redshift

load_dotenv()

TABLE = "customers"
CUSTOMERS_COL = os.getenv("customer_column")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")


def customers():
    """Extract customers data from Oracle and upload to S3"""
    print("Connecting to Oracle...")
    conn = get_connection()
    cur = conn.cursor()

    BATCH_DATE = get_batch_date_from_redshift()

    prepare_dblink(cur, BATCH_DATE)
    query = f"""
        SELECT {CUSTOMERS_COL}
        FROM {TABLE}@gokul_dblink
        WHERE UPDATE_TIMESTAMP >= TO_DATE('{BATCH_DATE}', 'YYYY-MM-DD')
    """

    df = pd.read_sql_query(query, conn, dtype_backend="pyarrow")
    print(f"Fetched {len(df)} rows from {TABLE}@gokul_dblink")

    s3_key = f"{TABLE.upper()}/{BATCH_DATE}/{TABLE}.csv"
    upload_to_s3(df, S3_BUCKET_NAME, s3_key)

    conn.close()
    print("Connection closed.")


if __name__ == "__main__":
    customers()
