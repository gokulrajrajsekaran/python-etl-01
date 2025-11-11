import subprocess
import os
from db_utils import insert_batch_log, update_batch_log

# Paths to your main ETL scripts
ETL_STAGES = [
    "source_to_s3/main.py",
    "s3_to_devstage/main.py",
    "devstage_to_devdw/main.py"
]


def run_stage(script_path):
    """Run one ETL stage and return True if successful."""
    print("=====================================================")
    print(f"Running ETL stage: {script_path}")
    print("=====================================================")

    result = subprocess.run(["python", script_path])
    return result.returncode == 0

def main():
    print("=====================================================")
    print("Starting FULL ETL PIPELINE: Source → S3 → DevStage → DevDW")
    print("=====================================================")

    # Step 1: Insert batch log (mark as running)
    insert_batch_log()

    try:
        # Step 2: Run all ETL stages one by one
        for stage in ETL_STAGES:
            success = run_stage(stage)
            if not success:
                print(f"ETL stage failed: {stage}")
                update_batch_log("F")
                print("ETL pipeline stopped due to failure.")
                return
            print(f"ETL stage completed successfully: {stage}")

        # Step 3: If all stages succeed → mark as Passed
        update_batch_log("P")
        print("All ETL stages completed successfully.")

    except Exception as e:
        print(f"Pipeline failed with error: {e}")
        update_batch_log("F")

    finally:
        print("ETL pipeline execution finished.")

if __name__ == "__main__":
    main()
