import pandas as pd
import numpy as np
import time

timestr = time.strftime("%Y%m%d-%H%M%S")
df_customer_transaction = pd.read_parquet("/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/customer_transaction_faultydates_removed.parquet")
print('Current time: ' + timestr)
print('Data Loaded...')

##Drop Faulty Date Entries
print('Dropping Duplicate Entries...')
print(df_customer_transaction.shape)
df_customer_transaction = df_customer_transaction.drop_duplicates()
print(df_customer_transaction.shape)
print('Dropping Duplicate Transaction IDs...')
df_customer_transaction.drop_duplicates(subset=['txn_id'], inplace=True)
print(df_customer_transaction.shape)
print(df_customer_transaction.nunique())
#Save Data
print('Saving Data...')
df_customer_transaction.to_parquet('/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/customer_transaction_duplicates_removed.parquet')
