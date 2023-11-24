import pandas as pd
import numpy as np

df_customer_transaction = pd.read_parquet("customer_transaction_faultydates_removed.parquet")
print('Data Loaded...')

#Fix Naming Conventions
nonAlphaNames = df_customer_transaction[df_customer_transaction['first_name'].str.isalpha()]
nonAlphaNames = nonAlphaNames[nonAlphaNames['last_name'].str.isalpha()]

nonAlphaNames.loc[:, 'first_name'] = nonAlphaNames['first_name'].str.capitalize()
nonAlphaNames.loc[:, 'last_name'] = nonAlphaNames['last_name'].str.capitalize()
print('Naming Conventions Fixed...')

#Save Data
print('Saving Data...')
df_customer_transaction.to_parquet('customer_transaction_fixed_namingconventions.parquet')
