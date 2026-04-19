import json

# Load notebook
with open('notebooks/02_cleaning.ipynb', encoding='utf-8') as f:
    nb = json.load(f)

# Clear outputs and execution counts properly
for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        cell['outputs'] = []
        cell['execution_count'] = None
    else:
        # Markdown cells don't have these fields
        if 'outputs' in cell:
            del cell['outputs']
        if 'execution_count' in cell:
            del cell['execution_count']

# Save cleaned notebook
with open('notebooks/02_cleaning.ipynb', 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1)

print('[OK] Cleared all cell outputs properly')
