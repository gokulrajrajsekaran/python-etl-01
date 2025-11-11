import pandas as pd
import os
import io
import boto3
import oracledb
import psycopg2
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("ORACLE_USER")
password = os.getenv("ORACLE_PASSWORD")
host = os.getenv("ORACLE_HOST")
port = os.getenv("ORACLE_PORT")
service = os.getenv("ORACLE_SERVICE")

def get_connection():
    """Connect to Oracle"""
    dsn = f"{host}:{port}/{service}"
    return oracledb.connect(user=user, password=password, dsn=dsn)

def prepare_dblink(cursor, batch_date):
    """Create PUBLIC DBLink for the given batch date"""
    if batch_date == "2001-01-01":
        remote_schema = "CM_20050609"
        remote_password = "CM_20050609123"
    else:
        remote_schema = f"CM_{batch_date.replace('-', '')}"
        remote_password = f"{remote_schema}123"

    link_name = "gokul_dblink"

    cursor.execute("ALTER SESSION SET CURRENT_SCHEMA = j25Gokulraj")
    try:
        cursor.execute(f"DROP PUBLIC DATABASE LINK {link_name}")
    except Exception:
        pass

    sql = f"""
    CREATE PUBLIC DATABASE LINK {link_name}
    CONNECT TO {remote_schema} IDENTIFIED BY "{remote_password}"
    USING '(DESCRIPTION=
      (ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))
      (CONNECT_DATA=(SERVICE_NAME={service}))
    )'
    """
    cursor.execute(sql)
    print(f"DBLink created for {remote_schema}")
    return link_name

def upload_to_s3(df, bucket_name, s3_key):
    s3_client = boto3.client("s3")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"Uploaded {s3_key} to S3 bucket '{bucket_name}'")

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

METADATA_SCHEMA = "j25gokulraj_etl_metadata"

def get_redshift_connection():
    """Connect to Redshift"""
    return psycopg2.connect(
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT
    )

def get_batch_date_from_redshift():
    """Fetch the latest ETL batch date from Redshift metadata table"""
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()
        cur.execute(f"""
            SELECT etl_batch_date 
            FROM {METADATA_SCHEMA}.batch_control
            ORDER BY etl_batch_no DESC
            LIMIT 1;
        """)
        result = cur.fetchone()
        if result:
            batch_date = str(result[0])
            print(f"Batch Date fetched from Redshift: {batch_date}")
            return batch_date
        else:
            raise Exception("No batch date found in batch_control table.")
    except Exception as e:
        print("Error fetching batch date:", e)
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def insert_batch_log():
    """Insert batch start log (status = 'R')"""
    conn = get_redshift_connection()
    cur = conn.cursor()
    try:
        sql = f"""
        INSERT INTO {METADATA_SCHEMA}.batch_control_log (
            etl_batch_no,
            etl_batch_date,
            etl_batch_status,
            etl_batch_start_time
        )
        SELECT
            etl_batch_no,
            etl_batch_date,
            'R',
            CURRENT_TIMESTAMP
        FROM {METADATA_SCHEMA}.batch_control
        ORDER BY etl_batch_no DESC
        LIMIT 1;
        """
        cur.execute(sql)
        conn.commit()
        print("Batch log inserted (Status = 'R')")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting batch log: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def update_batch_log(status):
    """Update latest batch record in batch_control_log"""
    conn = get_redshift_connection()
    cur = conn.cursor()
    try:
        sql = f"""
        UPDATE {METADATA_SCHEMA}.batch_control_log
        SET 
            etl_batch_status = %s,
            etl_batch_end_time = CURRENT_TIMESTAMP
        WHERE etl_batch_no = (
            SELECT MAX(etl_batch_no)
            FROM {METADATA_SCHEMA}.batch_control
        );
        """
        cur.execute(sql, (status,))
        conn.commit()
        print(f"Batch log updated (Status = '{status}')")
    except Exception as e:
        conn.rollback()
        print(f"Error updating batch log: {e}")
    finally:
        cur.close()
        conn.close()
