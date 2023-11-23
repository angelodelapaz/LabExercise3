import pandas as pd
import numpy as np

df_customer_transaction = pd.read_parquet("customer_transaction_duplicates_removed.parquet")
print('Data Loaded...')

##Drop Faulty Date Entries
print(df_customer_transaction.shape)
df_customer_transaction = df_customer_transaction.drop_duplicates()
print(df_customer_transaction.shape)
print('Dropped Faulty Dates...')

#Save Data
print('Saving Data...')
df_customer_transaction.to_parquet('customer_transaction_faultydates_removed.parquet')
