import subprocess
from dotenv import load_dotenv
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

load_dotenv()

alltable = os.getenv("alltable").split(",")


def main():
    for script in alltable:
        script = script.strip()   
        print(f"Running {script} ...")
        subprocess.run(["python", script])
    print("\nDEVSTAGE - DEVDW COMPLETED")

if __name__ == "__main__":
    main()