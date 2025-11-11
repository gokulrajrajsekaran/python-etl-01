import subprocess
from dotenv import load_dotenv
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

load_dotenv()

scripts = os.getenv("scripts")

if not scripts:
    print("No scripts found in .env file. Please add a 'scripts' variable.")
    exit(1)

script_list = [s.strip() for s in scripts.split(",") if s.strip()]

def main():
    print("===============================================")
    print("Starting S3 → Redshift ETL for All Tables")
    print("===============================================")

    success_count = 0
    failed_scripts = []

    for script in script_list:
        print(f"\n Running {script} ...")
        try:
            result = subprocess.run(["python", script], check=True)
            print(f" Completed: {script}")
            success_count += 1
        except subprocess.CalledProcessError:
            print(f" Failed: {script}")
            failed_scripts.append(script)
        except Exception as e:
            print(f" Unexpected error running {script}: {e}")
            failed_scripts.append(script)

    print("\n===============================================")
    print(" ETL Summary")
    print("===============================================")
    print(f" {success_count} tables loaded successfully.")
    if failed_scripts:
        print(f" Failed scripts: {', '.join(failed_scripts)}")
    else:
        print(" All ETL scripts ran successfully!")
    print("===============================================")

if __name__ == "__main__":
    main()
