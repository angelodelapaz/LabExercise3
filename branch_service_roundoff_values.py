import pandas as pd
import numpy as np

df_branch_service =  pd.read_parquet('branch_service_camelcase_values.parquet')
print('Data Loaded...')

#Round of prices to two decimal places
df_branch_service = df_branch_service.round({'price': 2})

## Saving Data
print('Saving Data...')
df_branch_service.to_parquet('branch_service_roundoff_values.parquet')