import pandas as pd
import numpy as np

#Import Data
print('Reading Data..')
df_branch_service = pd.read_parquet('branch_service_formatted_values.parquet')
df_customer_transaction = pd.read_parquet('finalcustomerinfo.parquet')

#Merge Data
df_merged = pd.merge(df_customer_transaction, df_branch_service)
print('Datasets Merged...')

#Save Data
print('Saving Data...')
df_merged.to_parquet('merged_data.parquet')