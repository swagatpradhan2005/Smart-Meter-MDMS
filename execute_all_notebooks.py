import subprocess
import json
import glob
import sys
from pathlib import Path

notebooks = []
for i in range(1, 6):
    files = glob.glob(f'notebooks/{i:02d}_*.ipynb')
    if files:
        notebooks.append(files[0])

if not notebooks:
    print("No notebooks found!")
    sys.exit(1)

print(f"Found {len(notebooks)} notebooks to execute\n")

for nb_file in notebooks:
    print(f"\n{'='*60}")
    print(f"EXECUTING: {nb_file}")
    print(f"{'='*60}")
    
    # Execute notebook with nbconvert
    cmd = [
        'jupyter', 'nbconvert',
        '--to', 'notebook',
        '--execute',
        '--inplace',
        '--ExecutePreprocessor.timeout=3600',
        nb_file
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"ERROR EXECUTING {nb_file}:")
        print(result.stderr)
        sys.exit(1)
    else:
        print(f"[OK] Execution completed")
    
    # Verify execution
    with open(nb_file) as f:
        nb = json.load(f)
    
    code_cells = [c for c in nb['cells'] if c['cell_type'] == 'code']
    executed_cells = [c for c in code_cells if c.get('execution_count') is not None]
    cells_with_output = [c for c in code_cells if c.get('outputs') and len(c['outputs']) > 0]
    
    print(f"Code cells: {len(code_cells)}")
    print(f"Executed cells (with execution_count): {len(executed_cells)}")
    print(f"Cells with output: {len(cells_with_output)}")
    
    if len(executed_cells) != len(code_cells):
        print(f"⚠ WARNING: Not all cells executed! {len(executed_cells)}/{len(code_cells)}")
    else:
        print(f"[OK] ALL CELLS EXECUTED")

print(f"\n{'='*60}")
print("EXECUTION VERIFICATION SUMMARY")
print(f"{'='*60}\n")

for nb_file in notebooks:
    with open(nb_file) as f:
        nb = json.load(f)
    
    code_cells = [c for c in nb['cells'] if c['cell_type'] == 'code']
    executed_cells = [c for c in code_cells if c.get('execution_count') is not None]
    
    status = "[OK] PASS" if len(executed_cells) == len(code_cells) else "✗ FAIL"
    print(f"{status}: {nb_file} ({len(executed_cells)}/{len(code_cells)} cells)")

print("\nNOTEBOOK EXECUTION PHASE 2 COMPLETE")
