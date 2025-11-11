import os
import sys
import psycopg2
from dotenv import load_dotenv

# Add parent path for db_utils import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_utils import get_batch_date_from_redshift

# Load environment variables
load_dotenv()

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

DEVDW_SCHEMA = "j25gokulraj_devdw"
TABLE = "monthly_customer_summary"
DAILY_TABLE = "daily_customer_summary"

def get_connection():
    """Create Redshift connection."""
    return psycopg2.connect(
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT
    )


def load_monthly_customer_summary(etl_batch_date):
    """Aggregate Monthly Customer Summary correctly."""
    conn = get_connection()
    cur = conn.cursor()

    print("======================================")
    print(f"Aggregating Monthly Customer Summary for batch_date >= {etl_batch_date}")
    print("======================================")

    try:
        # --- UPDATE existing monthly records ---
        update_sql = f"""
        UPDATE {DEVDW_SCHEMA}.{TABLE} m
        SET
            order_count = m.order_count + d.order_count,
            order_apd = m.order_apd + d.order_apd,
            order_apm = m.order_apm + d.order_apm,
            order_cost_amount = m.order_cost_amount + d.order_cost_amount,
            cancelled_order_count = m.cancelled_order_count + d.cancelled_order_count,
            cancelled_order_amount = m.cancelled_order_amount + d.cancelled_order_amount,
            cancelled_order_apd = m.cancelled_order_apd + d.cancelled_order_apd,
            cancelled_order_apm = m.cancelled_order_apm + d.cancelled_order_apm,
            shipped_order_count = m.shipped_order_count + d.shipped_order_count,
            shipped_order_amount = m.shipped_order_amount + d.shipped_order_amount,
            shipped_order_apd = m.shipped_order_apd + d.shipped_order_apd,
            shipped_order_apm = m.shipped_order_apm + d.shipped_order_apm,
            payment_apd = m.payment_apd + d.payment_apd,
            payment_apm = m.payment_apm + d.payment_apm,
            payment_amount = m.payment_amount + d.payment_amount,
            products_ordered_qty = m.products_ordered_qty + d.products_ordered_qty,
            products_items_qty = m.products_items_qty + d.products_items_qty,
            order_mrp_amount = m.order_mrp_amount + d.order_mrp_amount,
            new_customer_apd = m.new_customer_apd + d.new_customer_apd,
            new_customer_apm = m.new_customer_apm + d.new_customer_apm,
            new_customer_paid_apd = m.new_customer_paid_apd + d.new_customer_paid_apd,
            new_customer_paid_apm = m.new_customer_paid_apm + d.new_customer_paid_apm,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = d.etl_batch_no,
            etl_batch_date = d.etl_batch_date
        FROM (
            SELECT
                DATE_TRUNC('month', summary_date)::date AS start_of_the_month_date,
                dw_customer_id,
                SUM(order_count) AS order_count,
                SUM(order_apd) AS order_apd,
                COUNT(DISTINCT summary_date) AS order_apm,
                SUM(order_cost_amount) AS order_cost_amount,
                SUM(cancelled_order_count) AS cancelled_order_count,
                SUM(cancelled_order_amount) AS cancelled_order_amount,
                SUM(cancelled_order_apd) AS cancelled_order_apd,
                COUNT(DISTINCT CASE WHEN cancelled_order_count > 0 THEN summary_date END) AS cancelled_order_apm,
                SUM(shipped_order_count) AS shipped_order_count,
                SUM(shipped_order_amount) AS shipped_order_amount,
                SUM(shipped_order_apd) AS shipped_order_apd,
                COUNT(DISTINCT CASE WHEN shipped_order_count > 0 THEN summary_date END) AS shipped_order_apm,
                SUM(payment_apd) AS payment_apd,
                COUNT(DISTINCT CASE WHEN payment_amount > 0 THEN summary_date END) AS payment_apm,
                SUM(payment_amount) AS payment_amount,
                SUM(products_ordered_qty) AS products_ordered_qty,
                SUM(products_items_qty) AS products_items_qty,
                SUM(order_mrp_amount) AS order_mrp_amount,
                SUM(new_customer_apd) AS new_customer_apd,
                COUNT(DISTINCT CASE WHEN new_customer_apd > 0 THEN summary_date END) AS new_customer_apm,
                SUM(new_customer_paid_apd) AS new_customer_paid_apd,
                COUNT(DISTINCT CASE WHEN new_customer_paid_apd > 0 THEN summary_date END) AS new_customer_paid_apm,
                MAX(CAST(etl_batch_no AS INT)) AS etl_batch_no,
                MAX(etl_batch_date) AS etl_batch_date
            FROM {DEVDW_SCHEMA}.{DAILY_TABLE}
            WHERE etl_batch_date >= '{etl_batch_date}'
            GROUP BY 1, 2
        ) d
        WHERE m.start_of_the_month_date = d.start_of_the_month_date
          AND m.dw_customer_id = d.dw_customer_id;
        """
        cur.execute(update_sql)
        print("Updated existing monthly customer summary records.")

        # --- INSERT new monthly records ---
        insert_sql = f"""
        INSERT INTO {DEVDW_SCHEMA}.{TABLE} (
            start_of_the_month_date,
            dw_customer_id,
            order_count,
            order_apd,
            order_apm,
            order_cost_amount,
            cancelled_order_count,
            cancelled_order_amount,
            cancelled_order_apd,
            cancelled_order_apm,
            shipped_order_count,
            shipped_order_amount,
            shipped_order_apd,
            shipped_order_apm,
            payment_apd,
            payment_apm,
            payment_amount,
            products_ordered_qty,
            products_items_qty,
            order_mrp_amount,
            new_customer_apd,
            new_customer_apm,
            new_customer_paid_apd,
            new_customer_paid_apm,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            d.start_of_the_month_date,
            d.dw_customer_id,
            d.order_count,
            d.order_apd,
            d.order_apm,
            d.order_cost_amount,
            d.cancelled_order_count,
            d.cancelled_order_amount,
            d.cancelled_order_apd,
            d.cancelled_order_apm,
            d.shipped_order_count,
            d.shipped_order_amount,
            d.shipped_order_apd,
            d.shipped_order_apm,
            d.payment_apd,
            d.payment_apm,
            d.payment_amount,
            d.products_ordered_qty,
            d.products_items_qty,
            d.order_mrp_amount,
            d.new_customer_apd,
            d.new_customer_apm,
            d.new_customer_paid_apd,
            d.new_customer_paid_apm,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            d.etl_batch_no,
            d.etl_batch_date
        FROM (
            SELECT
                DATE_TRUNC('month', summary_date)::date AS start_of_the_month_date,
                dw_customer_id,
                SUM(order_count) AS order_count,
                SUM(order_apd) AS order_apd,
                COUNT(DISTINCT summary_date) AS order_apm,
                SUM(order_cost_amount) AS order_cost_amount,
                SUM(cancelled_order_count) AS cancelled_order_count,
                SUM(cancelled_order_amount) AS cancelled_order_amount,
                SUM(cancelled_order_apd) AS cancelled_order_apd,
                COUNT(DISTINCT CASE WHEN cancelled_order_count > 0 THEN summary_date END) AS cancelled_order_apm,
                SUM(shipped_order_count) AS shipped_order_count,
                SUM(shipped_order_amount) AS shipped_order_amount,
                SUM(shipped_order_apd) AS shipped_order_apd,
                COUNT(DISTINCT CASE WHEN shipped_order_count > 0 THEN summary_date END) AS shipped_order_apm,
                SUM(payment_apd) AS payment_apd,
                COUNT(DISTINCT CASE WHEN payment_amount > 0 THEN summary_date END) AS payment_apm,
                SUM(payment_amount) AS payment_amount,
                SUM(products_ordered_qty) AS products_ordered_qty,
                SUM(products_items_qty) AS products_items_qty,
                SUM(order_mrp_amount) AS order_mrp_amount,
                SUM(new_customer_apd) AS new_customer_apd,
                COUNT(DISTINCT CASE WHEN new_customer_apd > 0 THEN summary_date END) AS new_customer_apm,
                SUM(new_customer_paid_apd) AS new_customer_paid_apd,
                COUNT(DISTINCT CASE WHEN new_customer_paid_apd > 0 THEN summary_date END) AS new_customer_paid_apm,
                MAX(CAST(etl_batch_no AS INT)) AS etl_batch_no,
                MAX(etl_batch_date) AS etl_batch_date
            FROM {DEVDW_SCHEMA}.{DAILY_TABLE}
            WHERE etl_batch_date >= '{etl_batch_date}'
            GROUP BY 1, 2
        ) d
        LEFT JOIN {DEVDW_SCHEMA}.{TABLE} m
          ON m.start_of_the_month_date = d.start_of_the_month_date
         AND m.dw_customer_id = d.dw_customer_id
        WHERE m.dw_customer_id IS NULL;
        """
        cur.execute(insert_sql)
        conn.commit()
        print("Monthly Customer Summary aggregation completed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error while loading monthly customer summary: {e}")

    finally:
        cur.close()
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    batch_date = get_batch_date_from_redshift()
    load_monthly_customer_summary(batch_date)
