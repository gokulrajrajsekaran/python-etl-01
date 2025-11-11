import os
import sys
import psycopg2
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_utils import get_batch_date_from_redshift

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
REDSHIFT_IAM_ROLE = os.getenv("REDSHIFT_IAM_ROLE")

TABLE = "offices"  
BATCH_DATE = get_batch_date_from_redshift()


def get_connection():
    try:
        conn = psycopg2.connect(
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT
        )
        print("Connected to Redshift.")
        return conn
    except Exception as e:
        print("Connection failed:", e)
        raise

def load_s3_to_redshift():
    conn = get_connection()
    cur = conn.cursor()

    s3_path = f"s3://{S3_BUCKET_NAME}/{TABLE.upper()}/{BATCH_DATE}/{TABLE}.csv"
    print("======================================")
    print(f"Loading data from S3 to {TABLE}")
    print(f"S3 Path: {s3_path}")
    print("======================================")

    try:
        # Step 1: Clear old data
        truncate_sql = f"TRUNCATE TABLE {REDSHIFT_SCHEMA}.{TABLE};"
        cur.execute(truncate_sql)
        print(f"Old data cleared from {TABLE}")

        # Step 2: COPY new data
        copy_sql = f"""
            COPY {REDSHIFT_SCHEMA}.{TABLE}
            FROM '{s3_path}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            REGION '{AWS_REGION}'
            FORMAT AS CSV
            IGNOREHEADER 1
            DELIMITER ','
            TIMEFORMAT 'auto'
            TRUNCATECOLUMNS;
        """
        cur.execute(copy_sql)
        conn.commit()
        print(f"Data successfully loaded into {REDSHIFT_SCHEMA}.{TABLE}")

    except Exception as e:
        conn.rollback()
        print(f"Error during COPY for {TABLE}: {e}")

    finally:
        cur.close()
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    load_s3_to_redshift()
