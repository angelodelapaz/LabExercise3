from datetime import datetime
import pandas as pd
import numpy as np

df_branch_service =  pd.read_json('data/branch_service.json')

##Drop Duplicates
print('Dropping Duplicates...')
print(df_branch_service.shape)
df_branch_service = df_branch_service.drop_duplicates()
print(df_branch_service.shape)

## Saving Data
print('Saving Data...')
now = datetime.now()
df_branch_service.to_parquet()