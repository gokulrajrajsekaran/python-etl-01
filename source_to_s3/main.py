import subprocess
from dotenv import load_dotenv
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

load_dotenv()

scripts = os.getenv("scripts").split(",")

def main():
    for script in scripts:
        script = script.strip()   
        print(f"Running {script} ...")
        subprocess.run(["python", script])
    print("\nAll tables downloaded successfully!")

if __name__ == "__main__":
    main()