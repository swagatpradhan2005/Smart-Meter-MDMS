import subprocess
import sys
from pathlib import Path

os.chdir(r"c:\Smart Meter Data Systems")
subprocess.call([r".\.venv\Scripts\python.exe", "-m", "pip", "install", "--upgrade", "pip"])
subprocess.call([r".\.venv\Scripts\python.exe", "-m", "pip", "install", "ipykernel", "jupyter"])
subprocess.call([r".\.venv\Scripts\python.exe", "-m", "ipykernel", "install", "--user", "--name", "smart_meter_env", "--display-name", "Python (.venv Smart Meter)"])
print("Setup complete!")
