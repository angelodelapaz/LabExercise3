import pandas as pd
import numpy as np
import re
import time
timestr = time.strftime("%Y%m%d-%H%M%S")
print('Current time: ' + timestr)

df_DropDuplicated_Customers = pd.read_parquet("/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/customer_transaction_duplicates_removed.parquet")
print('Data Loaded...')
print('Removing Foreign Characters...')
#Fix Naming Conventions
df_DropDuplicated_Customers['last_name'] = df_DropDuplicated_Customers['last_name'].str.replace(r'[^a-z0-9]', '', regex=True, flags = re.IGNORECASE)
df_DropDuplicated_Customers['first_name'] = df_DropDuplicated_Customers['first_name'].str.replace(r'[^a-z0-9]', '', regex=True, flags = re.IGNORECASE)
df_DropDuplicated_Customers.to_parquet("/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/customer_transaction_cleaned_names.parquet")

print('Standardizing Naming Conventions...')

nonAlphaNames = df_DropDuplicated_Customers[df_DropDuplicated_Customers['first_name'].str.isalpha()]
nonAlphaNames = nonAlphaNames[nonAlphaNames['last_name'].str.isalpha()]
nonAlphaNames.loc[:, 'first_name'] = nonAlphaNames['first_name'].str.capitalize()
nonAlphaNames.loc[:, 'last_name'] = nonAlphaNames['last_name'].str.capitalize()
print('Naming Conventions Fixed...')

#Save Data
print('Saving Data...')
nonAlphaNames.to_parquet('/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/finalcustomerinfo.parquet.parquet')
