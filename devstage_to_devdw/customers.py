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
TABLE = "customers"

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

def load_incremental_customers():
    conn = get_connection()
    cur = conn.cursor()

   
    BATCH_DATE = get_batch_date_from_redshift()
    print("======================================")
    print(f"Incremental Load: DEVSTAGE → DEVDW ({TABLE})")
    print(f"Batch Date: {BATCH_DATE}")
    print("======================================")

    try:
        update_sql = f"""
        UPDATE {DEVDW_SCHEMA}.{TABLE} dw
        SET 
            customerName = st.customerName,
            contactLastName = st.contactLastName,
            contactFirstName = st.contactFirstName,
            phone = st.phone,
            addressLine1 = st.addressLine1,
            addressLine2 = st.addressLine2,
            city = st.city,
            state = st.state,
            postalCode = st.postalCode,
            country = st.country,
            salesRepEmployeeNumber = st.salesRepEmployeeNumber,
            creditLimit = st.creditLimit,
            src_update_timestamp = st.update_timestamp,
            dw_update_timestamp = GETDATE(),
            etl_batch_no = b.etl_batch_no,
            etl_batch_date = b.etl_batch_date
        FROM {DEVSTAGE_SCHEMA}.{TABLE} st
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        WHERE dw.src_customerNumber = st.customerNumber;
        """
        cur.execute(update_sql)
        print("Updated existing employee records where data changed.")

        insert_sql = f"""
        INSERT INTO {DEVDW_SCHEMA}.{TABLE} (
            src_customerNumber,
            customerName,
            contactLastName,
            contactFirstName,
            phone,
            addressLine1,
            addressLine2,
            city,
            state,
            postalCode,
            country,
            salesRepEmployeeNumber,
            creditLimit,
            src_create_timestamp,
            src_update_timestamp,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            st.customerNumber,
            st.customerName,
            st.contactLastName,
            st.contactFirstName,
            st.phone,
            st.addressLine1,
            st.addressLine2,
            st.city,
            st.state,
            st.postalCode,
            st.country,
            st.salesRepEmployeeNumber,
            st.creditLimit,
            st.create_timestamp,
            st.update_timestamp,
            GETDATE(),
            GETDATE(),
            b.etl_batch_no,
            b.etl_batch_date
        FROM {DEVSTAGE_SCHEMA}.{TABLE} st
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        LEFT JOIN {DEVDW_SCHEMA}.{TABLE} dw
          ON dw.src_customerNumber = st.customerNumber
        WHERE dw.src_customerNumber IS NULL;
        """
        cur.execute(insert_sql)
        conn.commit()
        print("Inserted new customer records where create_timestamp >= batch date.")

    except Exception as e:
        conn.rollback()
        print(f"Error during incremental load for {TABLE}: {e}")

    finally:
        cur.close()
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    load_incremental_customers()
