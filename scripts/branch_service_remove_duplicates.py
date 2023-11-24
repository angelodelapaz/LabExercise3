import pandas as pd
import numpy as np
import time
timestr = time.strftime("%Y%m%d-%H%M%S")
print('Current time: ' + timestr)
df_branch_service = pd.read_json("/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/branch_service_transaction_info.json")
print('Data Loaded...')

##Drop Duplicates
print('Dropping Duplicates...')
print(df_branch_service.shape)
df_branch_service = df_branch_service.drop_duplicates()
print(df_branch_service.shape)

print('Dropping Duplicate Transactions...')
df_branch_service.drop_duplicates(subset=['txn_id'], inplace=True)
print(df_branch_service.shape)
print(df_branch_service.nunique())
print('Duplicates Dropped...')

## Saving Data
print('Saving Data...')
df_branch_service.to_parquet('/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/branch_service_duplicates_removed.parquet')