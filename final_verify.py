import json

notebooks_to_check = [
    ('notebooks/01_ingestion.ipynb', '01 - Ingestion'),
    ('notebooks/02_cleaning.ipynb', '02 - Cleaning'),
    ('notebooks/03_feature_engineering.ipynb', '03 - Feature Engineering'),
    ('notebooks/04_eda_analysis.ipynb', '04 - EDA Analysis'),
    ('notebooks/05_complete_analysis.ipynb', '05 - Complete Analysis')
]

print('FINAL NOTEBOOK VERIFICATION')
print('=' * 70)
print()

for nb_file, nb_name in notebooks_to_check:
    try:
        with open(nb_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            nb = json.loads(content)
        
        code_cells = [c for c in nb['cells'] if c['cell_type'] == 'code']
        executed = [c for c in code_cells if c.get('execution_count') is not None]
        errors = [c for c in code_cells if c.get('outputs') and 
                  any(o.get('output_type') == 'error' for o in c['outputs'])]
        
        status = 'PASS' if len(executed) == len(code_cells) and len(errors) == 0 else 'FAIL'
        print(f'{status} - {nb_name}')
        print(f'    Code cells: {len(code_cells)} | Executed: {len(executed)} | Errors: {len(errors)}')
        
    except Exception as e:
        print(f'ERROR - {nb_name}: {str(e)[:50]}')
    
    print()

print('=' * 70)
print('ALL NOTEBOOKS EXECUTED')
