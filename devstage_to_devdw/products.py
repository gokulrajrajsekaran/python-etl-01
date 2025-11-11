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
TABLE = "products"

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

def load_incremental_products():
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
            productName = s.productName,
            productLine = s.productLine,
            productScale = s.productScale,
            productVendor = s.productVendor,
            productDescription = s.productDescription,
            quantityInStock = s.quantityInStock,
            buyPrice = s.buyPrice,
            msrp = s.msrp,
            src_update_timestamp = s.update_timestamp,
            dw_update_timestamp = GETDATE(),
            etl_batch_no = b.etl_batch_no,
            etl_batch_date = b.etl_batch_date
        FROM {DEVSTAGE_SCHEMA}.{TABLE} s
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        WHERE d.src_productCode = s.productCode;
        """
        cur.execute(update_sql)
        print("Updated existing product records where data changed.")

        insert_sql = f"""
        INSERT INTO {DEVDW_SCHEMA}.{TABLE} (
            src_productCode,
            productName,
            productLine,
            productScale,
            productVendor,
            productDescription,
            quantityInStock,
            buyPrice,
            msrp,
            src_create_timestamp,
            src_update_timestamp,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            s.productCode,
            s.productName,
            s.productLine,
            s.productScale,
            s.productVendor,
            s.productDescription,
            s.quantityInStock,
            s.buyPrice,
            s.msrp,
            s.create_timestamp,
            s.update_timestamp,
            GETDATE(),
            GETDATE(),
            b.etl_batch_no,
            b.etl_batch_date
        FROM {DEVSTAGE_SCHEMA}.{TABLE} s
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        LEFT JOIN {DEVDW_SCHEMA}.{TABLE} d
          ON s.productCode = d.src_productCode
        WHERE d.src_productCode IS NULL;
        """
        cur.execute(insert_sql)
        conn.commit()
        print("Inserted new product records successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error during incremental load for {TABLE}: {e}")

    finally:
        cur.close()
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    load_incremental_products()
