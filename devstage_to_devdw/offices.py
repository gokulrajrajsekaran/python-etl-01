import os
import sys
import psycopg2
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_utils import get_batch_date_from_redshift

load_dotenv()


REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

DEVSTAGE_SCHEMA = "j25gokulraj_devstage"
DEVDW_SCHEMA = "j25gokulraj_devdw"
TABLE = "offices"

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

def load_incremental_offices():
    conn = get_connection()
    cur = conn.cursor()

    # Get batch date from metadata
    BATCH_DATE = get_batch_date_from_redshift()
    print("======================================")
    print(f"Incremental Load: DEVSTAGE → DEVDW ({TABLE})")
    print(f"Batch Date: {BATCH_DATE}")
    print("======================================")

    try:
        update_sql = f"""
        UPDATE {DEVDW_SCHEMA}.{TABLE} d
        SET 
            city = s.city,
            phone = s.phone,
            addressLine1 = s.addressLine1,
            addressLine2 = s.addressLine2,
            state = s.state,
            country = s.country,
            postalCode = s.postalCode,
            territory = s.territory,
            src_update_timestamp = s.update_timestamp,
            dw_update_timestamp = GETDATE(),
            etl_batch_no = b.etl_batch_no,
            etl_batch_date = b.etl_batch_date
        FROM {DEVSTAGE_SCHEMA}.{TABLE} s
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        WHERE d.officeCode = s.officeCode;
        """
        cur.execute(update_sql)
        print("Updated existing office records where data changed.")

        insert_sql = f"""
        INSERT INTO {DEVDW_SCHEMA}.{TABLE} (
            officeCode,
            city,
            phone,
            addressLine1,
            addressLine2,
            state,
            country,
            postalCode,
            territory,
            src_create_timestamp,
            src_update_timestamp,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            s.officeCode,
            s.city,
            s.phone,
            s.addressLine1,
            s.addressLine2,
            s.state,
            s.country,
            s.postalCode,
            s.territory,
            s.create_timestamp,
            s.update_timestamp,
            GETDATE(),
            GETDATE(),
            b.etl_batch_no,
            b.etl_batch_date
        FROM {DEVSTAGE_SCHEMA}.{TABLE} s
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        LEFT JOIN {DEVDW_SCHEMA}.{TABLE} d
          ON s.officeCode = d.officeCode
        WHERE d.officeCode IS NULL;
        """
        cur.execute(insert_sql)

        conn.commit()
        print("Inserted new office records successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error during incremental load for {TABLE}: {e}")

    finally:
        cur.close()
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    load_incremental_offices()
