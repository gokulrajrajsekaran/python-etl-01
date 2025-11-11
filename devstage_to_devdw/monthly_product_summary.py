import os
import sys
import psycopg2
from dotenv import load_dotenv

# Add parent path for db_utils import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_utils import get_batch_date_from_redshift

# Load environment variable
load_dotenv()

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

DEVDW_SCHEMA = "j25gokulraj_devdw"
TABLE = "monthly_product_summary"


def get_connection():
    """Create a connection to Redshift."""
    return psycopg2.connect(
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT
    )


def load_monthly_product_summary():
    """Aggregate Monthly Product Summary correctly."""
    conn = get_connection()
    cur = conn.cursor()
    BATCH_DATE = get_batch_date_from_redshift()

    print("======================================")
    print(f"Aggregating Monthly Product Summary for batch_date >= {BATCH_DATE}")
    print("======================================")

    try:
        sql = f"""
        -- Step 1️ : UPDATE existing monthly records
        UPDATE {DEVDW_SCHEMA}.{TABLE}
        SET 
            customer_apd = {DEVDW_SCHEMA}.{TABLE}.customer_apd + d.customer_apd,
            customer_apm = {DEVDW_SCHEMA}.{TABLE}.customer_apm + d.customer_apm,
            product_cost_amount = {DEVDW_SCHEMA}.{TABLE}.product_cost_amount + d.product_cost_amount,
            product_mrp_amount = {DEVDW_SCHEMA}.{TABLE}.product_mrp_amount + d.product_mrp_amount,
            cancelled_product_qty = {DEVDW_SCHEMA}.{TABLE}.cancelled_product_qty + d.cancelled_product_qty,
            cancelled_cost_amount = {DEVDW_SCHEMA}.{TABLE}.cancelled_cost_amount + d.cancelled_cost_amount,
            cancelled_mrp_amount = {DEVDW_SCHEMA}.{TABLE}.cancelled_mrp_amount + d.cancelled_mrp_amount,
            cancelled_order_apd = {DEVDW_SCHEMA}.{TABLE}.cancelled_order_apd + d.cancelled_order_apd,
            cancelled_order_apm = {DEVDW_SCHEMA}.{TABLE}.cancelled_order_apm + d.cancelled_order_apm,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = d.etl_batch_no,
            etl_batch_date = d.etl_batch_date
        FROM (
            SELECT 
                DATE_TRUNC('month', o.orderDate) AS start_of_the_month_date,
                od.dw_product_id,
                COUNT(DISTINCT o.dw_customer_id) AS customer_apd,
                1 AS customer_apm,
                SUM(od.priceEach * od.quantityOrdered) AS product_cost_amount,
                SUM(p.MSRP * od.quantityOrdered) AS product_mrp_amount,
                SUM(CASE WHEN LOWER(TRIM(o.status)) = 'cancelled' THEN od.quantityOrdered ELSE 0 END) AS cancelled_product_qty,
                SUM(CASE WHEN LOWER(TRIM(o.status)) = 'cancelled' THEN od.priceEach * od.quantityOrdered ELSE 0 END) AS cancelled_cost_amount,
                SUM(CASE WHEN LOWER(TRIM(o.status)) = 'cancelled' THEN p.MSRP * od.quantityOrdered ELSE 0 END) AS cancelled_mrp_amount,
                COUNT(DISTINCT CASE WHEN LOWER(TRIM(o.status)) = 'cancelled' THEN o.dw_order_id END) AS cancelled_order_apd,
                COUNT(DISTINCT CASE WHEN LOWER(TRIM(o.status)) = 'cancelled' THEN DATE_TRUNC('month', o.cancelledDate) END) AS cancelled_order_apm,
                MAX(CAST(o.etl_batch_no AS INT)) AS etl_batch_no,
                MAX(o.etl_batch_date) AS etl_batch_date
            FROM {DEVDW_SCHEMA}.orders o
            JOIN {DEVDW_SCHEMA}.orderdetails od ON o.dw_order_id = od.dw_order_id
            JOIN {DEVDW_SCHEMA}.products p ON od.dw_product_id = p.dw_product_id
            WHERE (CAST(o.orderDate AS DATE) >= '{BATCH_DATE}' OR CAST(o.cancelledDate AS DATE) >= '{BATCH_DATE}')
            GROUP BY 1, 2
        ) d
        WHERE {DEVDW_SCHEMA}.{TABLE}.start_of_the_month_date = d.start_of_the_month_date
          AND {DEVDW_SCHEMA}.{TABLE}.dw_product_id = d.dw_product_id;

        -- Step 2️ : INSERT new monthly records
        INSERT INTO {DEVDW_SCHEMA}.{TABLE} (
            start_of_the_month_date,
            dw_product_id,
            customer_apd,
            customer_apm,
            product_cost_amount,
            product_mrp_amount,
            cancelled_product_qty,
            cancelled_cost_amount,
            cancelled_mrp_amount,
            cancelled_order_apd,
            cancelled_order_apm,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            d.start_of_the_month_date,
            d.dw_product_id,
            d.customer_apd,
            d.customer_apm,
            d.product_cost_amount,
            d.product_mrp_amount,
            d.cancelled_product_qty,
            d.cancelled_cost_amount,
            d.cancelled_mrp_amount,
            d.cancelled_order_apd,
            d.cancelled_order_apm,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            d.etl_batch_no,
            d.etl_batch_date
        FROM (
            SELECT 
                DATE_TRUNC('month', o.orderDate) AS start_of_the_month_date,
                od.dw_product_id,
                COUNT(DISTINCT o.dw_customer_id) AS customer_apd,
                1 AS customer_apm,
                SUM(od.priceEach * od.quantityOrdered) AS product_cost_amount,
                SUM(p.MSRP * od.quantityOrdered) AS product_mrp_amount,
                SUM(CASE WHEN LOWER(TRIM(o.status)) = 'cancelled' THEN od.quantityOrdered ELSE 0 END) AS cancelled_product_qty,
                SUM(CASE WHEN LOWER(TRIM(o.status)) = 'cancelled' THEN od.priceEach * od.quantityOrdered ELSE 0 END) AS cancelled_cost_amount,
                SUM(CASE WHEN LOWER(TRIM(o.status)) = 'cancelled' THEN p.MSRP * od.quantityOrdered ELSE 0 END) AS cancelled_mrp_amount,
                COUNT(DISTINCT CASE WHEN LOWER(TRIM(o.status)) = 'cancelled' THEN o.dw_order_id END) AS cancelled_order_apd,
                COUNT(DISTINCT CASE WHEN LOWER(TRIM(o.status)) = 'cancelled' THEN DATE_TRUNC('month', o.cancelledDate) END) AS cancelled_order_apm,
                MAX(CAST(o.etl_batch_no AS INT)) AS etl_batch_no,
                MAX(o.etl_batch_date) AS etl_batch_date
            FROM {DEVDW_SCHEMA}.orders o
            JOIN {DEVDW_SCHEMA}.orderdetails od ON o.dw_order_id = od.dw_order_id
            JOIN {DEVDW_SCHEMA}.products p ON od.dw_product_id = p.dw_product_id
            WHERE (CAST(o.orderDate AS DATE) >= '{BATCH_DATE}' OR CAST(o.cancelledDate AS DATE) >= '{BATCH_DATE}')
            GROUP BY 1, 2
        ) d
        LEFT JOIN {DEVDW_SCHEMA}.{TABLE}
          ON {DEVDW_SCHEMA}.{TABLE}.start_of_the_month_date = d.start_of_the_month_date
         AND {DEVDW_SCHEMA}.{TABLE}.dw_product_id = d.dw_product_id
        WHERE {DEVDW_SCHEMA}.{TABLE}.dw_product_id IS NULL;
        """

        cur.execute(sql)
        conn.commit()
        print("Monthly Product Summary aggregated successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error while loading monthly product summary: {e}")
    finally:
        cur.close()
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    load_monthly_product_summary()
