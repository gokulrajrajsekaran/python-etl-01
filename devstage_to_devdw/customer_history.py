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

DEVDW_SCHEMA = "j25gokulraj_devdw"
TABLE = "customer_history"

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

def maintain_customer_history():
    conn = get_connection()
    cur = conn.cursor()

    BATCH_DATE = get_batch_date_from_redshift()
    print("======================================")
    print("Maintaining Customer History")
    print(f"Batch Date: {BATCH_DATE}")
    print("======================================")

    try:
        
        update_sql = f"""
        UPDATE {DEVDW_SCHEMA}.{TABLE} h
        SET 
            effective_to_date = DATEADD(day, -1, b.etl_batch_date),
            dw_active_record_ind = 0,
            dw_update_timestamp = GETDATE(),
            update_etl_batch_no = b.etl_batch_no,
            update_etl_batch_date = b.etl_batch_date
        FROM {DEVDW_SCHEMA}.customers c
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        WHERE h.dw_customer_id = c.dw_customer_id
          AND h.dw_active_record_ind = 1
          AND COALESCE(h.creditLimit, 0) <> COALESCE(c.creditLimit, 0);
        """
        cur.execute(update_sql)
        print("Closed old customer history records where credit limit changed.")

        # Step 2️: Insert new active record for changed or new customers
        insert_sql = f"""
        INSERT INTO {DEVDW_SCHEMA}.{TABLE} (
            dw_customer_id,
            creditLimit,
            effective_from_date,
            effective_to_date,
            dw_active_record_ind,
            dw_create_timestamp,
            dw_update_timestamp,
            create_etl_batch_no,
            create_etl_batch_date,
            update_etl_batch_no,
            update_etl_batch_date
        )
        SELECT 
            c.dw_customer_id,
            c.creditLimit,
            b.etl_batch_date AS effective_from_date,
            NULL AS effective_to_date,
            1 AS dw_active_record_ind,
            GETDATE() AS dw_create_timestamp,
            GETDATE() AS dw_update_timestamp,
            b.etl_batch_no AS create_etl_batch_no,
            b.etl_batch_date AS create_etl_batch_date,
            NULL AS update_etl_batch_no,
            NULL AS update_etl_batch_date
        FROM {DEVDW_SCHEMA}.customers c
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        LEFT JOIN {DEVDW_SCHEMA}.{TABLE} h
          ON c.dw_customer_id = h.dw_customer_id
         AND h.dw_active_record_ind = 1
        WHERE h.dw_customer_id IS NULL
           OR COALESCE(h.creditLimit, 0) <> COALESCE(c.creditLimit, 0);
        """
        cur.execute(insert_sql)

        conn.commit()
        print("Customer history maintained successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error maintaining customer history: {e}")

    finally:
        cur.close()
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    maintain_customer_history()
