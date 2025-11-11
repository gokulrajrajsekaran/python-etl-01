import os
import sys
import psycopg2
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_utils import get_batch_date_from_redshift

#load env
load_dotenv()

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

DEVSTAGE_SCHEMA = "j25gokulraj_devstage"
DEVDW_SCHEMA = "j25gokulraj_devdw"
TABLE = "employees"


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

def load_incremental_employees():
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
            lastName = s.lastName,
            firstName = s.firstName,
            extension = s.extension,
            email = s.email,
            officeCode = s.officeCode,
            reportsTo = s.reportsTo,
            jobTitle = s.jobTitle,
            src_update_timestamp = s.update_timestamp,
            dw_update_timestamp = GETDATE(),
            etl_batch_no = b.etl_batch_no,
            etl_batch_date = b.etl_batch_date
        FROM {DEVSTAGE_SCHEMA}.{TABLE} s
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        WHERE d.employeeNumber = s.employeeNumber;
        """
        cur.execute(update_sql)
        print("Step 1: Updated existing employees.")

        insert_sql = f"""
        INSERT INTO {DEVDW_SCHEMA}.{TABLE} (
            employeeNumber,
            lastName,
            firstName,
            extension,
            email,
            officeCode,
            reportsTo,
            jobTitle,
            dw_office_id,
            dw_reporting_employee_id,
            src_create_timestamp,
            src_update_timestamp,
            dw_create_timestamp,
            dw_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            s.employeeNumber,
            s.lastName,
            s.firstName,
            s.extension,
            s.email,
            s.officeCode,
            s.reportsTo,
            s.jobTitle,
            o.dw_office_id,
            NULL AS dw_reporting_employee_id,
            s.create_timestamp,
            s.update_timestamp,
            GETDATE(),
            GETDATE(),
            b.etl_batch_no,
            b.etl_batch_date
        FROM {DEVSTAGE_SCHEMA}.{TABLE} s
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        LEFT JOIN {DEVDW_SCHEMA}.offices o
            ON s.officeCode = o.officeCode
        LEFT JOIN {DEVDW_SCHEMA}.{TABLE} d
            ON s.employeeNumber = d.employeeNumber
        WHERE d.employeeNumber IS NULL;
        """
        cur.execute(insert_sql)
        print("Step 2: Inserted new employees.")

        reporting_update_sql = f"""
        UPDATE {DEVDW_SCHEMA}.{TABLE} e
        SET dw_reporting_employee_id = r.dw_employee_id
        FROM {DEVDW_SCHEMA}.{TABLE} r
        CROSS JOIN j25gokulraj_etl_metadata.batch_control b
        WHERE e.reportsTo = r.employeeNumber;
        """
        cur.execute(reporting_update_sql)
        print("Step 3: Updated dw_reporting_employee_id for reporting hierarchy.")

        # Commit all changes
        conn.commit()
        print("All ETL steps executed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error during incremental load for {TABLE}: {e}")

    finally:
        cur.close()
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    load_incremental_employees()
