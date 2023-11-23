import pandas as pd
import numpy as np

df_branch_service =  pd.read_parquet('branch_service_nullbranch_removed.parquet')
print('Data Loaded...')

#Change Values to Camel Case
df_branch_service.loc[df_branch_service['branch_name'] == 'Starmall', 'branch_name'] = 'StarMall'
df_branch_service.loc[df_branch_service['branch_name'] == 'Megamall', 'branch_name'] = 'MegaMall'

## Saving Data
print('Saving Data...')
df_branch_service.to_parquet('branch_service_camelcase_values.parquet')