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
TABLE = "orders"

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

def load_incremental_orders():
    conn = get_connection()
    cur = conn.cursor()

    # Get batch date
    BATCH_DATE = get_batch_date_from_redshift()
    print("======================================")
    print(f"Incremental Load: DEVSTAGE → DEVDW ({TABLE})")
    print(f"Batch Date: {BATCH_DATE}")
    print("======================================")

    try:
        update_sql = f"""
        UPDATE {DEVDW_SCHEMA}.{TABLE} d
        SET
            orderDate = s.orderDate,
            requiredDate = s.requiredDate,
            shippedDate = s.shippedDate,
            status = s.status,
            comments = s.comments,
            cancelledDate = s.cancelledDate,
            src_customerNumber = s.customerNumber,
            dw_customer_id = c.dw_customer_id,
            src_update_timestamp = s.update_timestamp,
            dw_update_timestamp = GETDATE(),
            etl_batch_no = b.etl_batch_no,
            etl_batch_date = b.etl_batch_date
        FROM {DEVSTAGE_SCHEMA}.{TABLE} s
        LEFT JOIN {DEVDW_SCHEMA}.customers c
            ON s.customerNumber = c.src_customerNumber
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        WHERE d.src_orderNumber = s.orderNumber;
        """
        cur.execute(update_sql)
        print("Updated existing order records where data changed.")

        insert_sql = f"""
        INSERT INTO {DEVDW_SCHEMA}.{TABLE} (
            dw_customer_id,
            src_orderNumber,
            orderDate,
            requiredDate,
            shippedDate,
            status,
            comments,
            src_customerNumber,
            src_create_timestamp,
            src_update_timestamp,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date,
            cancelledDate
        )
        SELECT
            c.dw_customer_id,
            s.orderNumber,
            s.orderDate,
            s.requiredDate,
            s.shippedDate,
            s.status,
            s.comments,
            s.customerNumber,
            s.create_timestamp,
            s.update_timestamp,
            GETDATE(),
            GETDATE(),
            b.etl_batch_no,
            b.etl_batch_date,
            s.cancelledDate
        FROM {DEVSTAGE_SCHEMA}.{TABLE} s
        LEFT JOIN {DEVDW_SCHEMA}.{TABLE} d
          ON s.orderNumber = d.src_orderNumber
        LEFT JOIN {DEVDW_SCHEMA}.customers c
          ON s.customerNumber = c.src_customerNumber
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        WHERE d.src_orderNumber IS NULL;
        """
        cur.execute(insert_sql)

        conn.commit()
        print("Inserted new order records successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error during incremental load for {TABLE}: {e}")

    finally:
        cur.close()
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    load_incremental_orders()
