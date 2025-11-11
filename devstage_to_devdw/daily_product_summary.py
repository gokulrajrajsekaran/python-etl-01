import os
import sys
import psycopg2
from dotenv import load_dotenv

# Add parent path for db_utils import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_utils import get_batch_date_from_redshift

load_dotenv()

# Redshift credentials
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

# Schema/Table
DEVDW_SCHEMA = "j25Gokulraj_devdw"
TABLE = "daily_product_summary"

def get_connection():
    """Create connection to Redshift."""
    return psycopg2.connect(
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT
    )

def load_daily_product_summary():
    """Load Daily Product Summary using provided SQL logic."""
    conn = get_connection()
    cur = conn.cursor()

    # Get batch date dynamically from Redshift metadata
    etl_batch_date = get_batch_date_from_redshift()  # e.g. 2001-01-01
    etl_batch_no = 123  # You can replace with dynamic fetch if needed

    # Convert to string if datetime object
    etl_batch_date_str = (
        etl_batch_date if isinstance(etl_batch_date, str)
        else etl_batch_date.strftime('%Y-%m-%d')
    )

    print("======================================")
    print(f"Loading Daily Product Summary for {etl_batch_date_str}")
    print("======================================")

    # === Main SQL Script (exact logic from your SQL ===
    sql = f"""
    INSERT INTO {DEVDW_SCHEMA}.{TABLE} (
        summary_date,
        dw_product_id,
        customer_apd,
        product_cost_amount,
        product_mrp_amount,
        cancelled_product_qty,
        cancelled_cost_amount,
        cancelled_mrp_amount,
        cancelled_order_apd,
        dw_create_timestamp,
        dw_update_timestamp,
        etl_batch_no,
        etl_batch_date
    )
    WITH product_sales_cte AS (
        SELECT
            CAST(o.orderDate AS DATE) AS summary_date,
            od.dw_product_id,
            COUNT(DISTINCT o.dw_customer_id) AS customer_apd,
            SUM(od.priceEach * od.quantityOrdered) AS product_cost_amount,
            SUM(p.MSRP * od.quantityOrdered) AS product_mrp_amount,
            0 AS cancelled_product_qty,
            0 AS cancelled_cost_amount,
            0 AS cancelled_mrp_amount,
            0 AS cancelled_order_apd
        FROM {DEVDW_SCHEMA}.orders o
        JOIN {DEVDW_SCHEMA}.orderdetails od ON o.dw_order_id = od.dw_order_id
        JOIN {DEVDW_SCHEMA}.products p ON od.dw_product_id = p.dw_product_id
        WHERE CAST(o.orderDate AS DATE) >= '{etl_batch_date_str}'
        GROUP BY summary_date, od.dw_product_id
    ),
    cancelled_products_cte AS (
        SELECT
            CAST(o.cancelledDate AS DATE) AS summary_date,
            od.dw_product_id,
            0 AS customer_apd,
            0 AS product_cost_amount,
            0 AS product_mrp_amount,
            SUM(od.quantityOrdered) AS cancelled_product_qty,
            SUM(od.priceEach * od.quantityOrdered) AS cancelled_cost_amount,
            SUM(p.MSRP * od.quantityOrdered) AS cancelled_mrp_amount,
            COUNT(DISTINCT o.dw_order_id) AS cancelled_order_apd
        FROM {DEVDW_SCHEMA}.orders o
        JOIN {DEVDW_SCHEMA}.orderdetails od ON o.dw_order_id = od.dw_order_id
        JOIN {DEVDW_SCHEMA}.products p ON od.dw_product_id = p.dw_product_id
        WHERE LOWER(TRIM(o.status)) = 'cancelled'
          AND CAST(o.cancelledDate AS DATE) >= '{etl_batch_date_str}'
        GROUP BY summary_date, od.dw_product_id
    ),
    combined_cte AS (
        SELECT * FROM product_sales_cte
        UNION ALL
        SELECT * FROM cancelled_products_cte
    )
    SELECT
        summary_date,
        dw_product_id,
        MAX(customer_apd) AS customer_apd,
        MAX(product_cost_amount) AS product_cost_amount,
        MAX(product_mrp_amount) AS product_mrp_amount,
        MAX(cancelled_product_qty) AS cancelled_product_qty,
        MAX(cancelled_cost_amount) AS cancelled_cost_amount,
        MAX(cancelled_mrp_amount) AS cancelled_mrp_amount,
        MAX(cancelled_order_apd) AS cancelled_order_apd,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        {etl_batch_no} AS etl_batch_no,
        '{etl_batch_date_str}' AS etl_batch_date
    FROM combined_cte
    GROUP BY summary_date, dw_product_id;
    """

    try:
        cur.execute(sql)
        conn.commit()
        print("Daily Product Summary loaded successfully.")

        # Verify record count
        count_query = f"""
        SELECT COUNT(*)
        FROM {DEVDW_SCHEMA}.{TABLE}
        WHERE etl_batch_date = '{etl_batch_date_str}';
        """
        cur.execute(count_query)
        row_count = cur.fetchone()[0]
        print(f"Rows inserted for {etl_batch_date_str}: {row_count}")

    except Exception as e:
        conn.rollback()
        print(f"Error loading daily product summary: {e}")

    finally:
        cur.close()
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    load_daily_product_summary()
