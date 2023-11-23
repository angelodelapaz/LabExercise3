import pandas as pd
import numpy as np

df_branch_service = pd.read_json("branch_service_transaction_info.json")
print('Data Loaded...')

##Drop Duplicates
print('Dropping Duplicates...')
print(df_branch_service.shape)
df_branch_service = df_branch_service.drop_duplicates()
print(df_branch_service.shape)

#Drop Almost Exact Duplicates
cols = df_branch_service.columns.tolist()
cols.remove('txn_id')

for col in cols:
    observed_cols = df_branch_service.drop(col, axis=1).columns.tolist()
    df_branch_service.drop_duplicates(observed_cols, keep='first', inplace=True)

print(df_branch_service.shape)
print(df_branch_service.nunique())

## Saving Data
print('Saving Data...')
df_branch_service.to_parquet('branch_service_duplicates_removed.parquet')