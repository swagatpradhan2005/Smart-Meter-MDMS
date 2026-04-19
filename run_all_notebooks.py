#!/usr/bin/env python
"""Execute all 5 notebooks sequentially with error handling."""

import subprocess
import os
import sys
from pathlib import Path
from datetime import datetime

os.chdir(r'c:\Smart Meter Data Systems')

notebooks = [
    'notebooks/01_ingestion.ipynb',
    'notebooks/02_cleaning.ipynb',
    'notebooks/03_feature_engineering.ipynb',
    'notebooks/04_eda_analysis.ipynb',
    'notebooks/05_complete_analysis.ipynb',
]

print("\n" + "="*80)
print("EXECUTING ALL 5 NOTEBOOKS SEQUENTIALLY")
print("="*80)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

success_count = 0
failed_notebooks = []

for i, nb in enumerate(notebooks, 1):
    print(f"\n[{i}/5] Running {nb}...")
    print("-" * 60)
    
    try:
        # Use jupyter nbconvert to run notebook
        cmd = [
            sys.executable, '-m', 'jupyter', 'nbconvert',
            '--to', 'notebook',
            '--execute',
            '--ExecutePreprocessor.timeout=600',
            '--output', nb,
            nb
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"[OK] {nb} executed successfully")
            success_count += 1
        else:
            print(f"[ERROR] {nb} failed with return code {result.returncode}")
            if result.stderr:
                print("STDERR:", result.stderr[:500])
            failed_notebooks.append(nb)
    except Exception as e:
        print(f"[ERROR] Exception running {nb}: {str(e)}")
        failed_notebooks.append(nb)

print("\n" + "="*80)
print("EXECUTION SUMMARY")
print("="*80)
print(f"Completed: {success_count}/5 notebooks")
print(f"Failed: {len(failed_notebooks)}/5 notebooks")

if failed_notebooks:
    print("\nFailed notebooks:")
    for nb in failed_notebooks:
        print(f"  - {nb}")
else:
    print("\n[OK] ALL NOTEBOOKS EXECUTED SUCCESSFULLY!")

print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
