import pandas as pd
import os
import io
import boto3
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_utils import get_connection, prepare_dblink, upload_to_s3,get_batch_date_from_redshift
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

TABLE = "offices"
OFFICES_COL = os.getenv("offices_column")
BATCH_DATE = os.getenv("BATCH_DATE")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")



def offices():
    """Extract offices data from Oracle and upload to S3"""
    print("Connecting to Oracle...")
    conn = get_connection()
    cur = conn.cursor()
    BATCH_DATE = get_batch_date_from_redshift()
    # Create DBLink for this batch date
    prepare_dblink(cur, BATCH_DATE)

    # Query remote schema via DBLink
    query = f"""
        SELECT {OFFICES_COL}
        FROM {TABLE}@gokul_dblink
        WHERE UPDATE_TIMESTAMP >= TO_DATE('{BATCH_DATE}', 'YYYY-MM-DD')
    """

    # Read into DataFrame
    df = pd.read_sql_query(query, conn,dtype_backend="pyarrow")
    print(f"Fetched {len(df)} rows from {TABLE}@gokul_dblink")

    # Upload to S3
    s3_key = f"{TABLE.upper()}/{BATCH_DATE}/{TABLE}.csv"
    upload_to_s3(df, S3_BUCKET_NAME, s3_key)

    conn.close()
    print("Connection closed.")


if __name__ == "__main__":
    offices()
