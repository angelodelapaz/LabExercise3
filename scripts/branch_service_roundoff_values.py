import pandas as pd
import numpy as np
import time
timestr = time.strftime("%Y%m%d-%H%M%S")
print('Current time: ' + timestr)
df_branch_service =  pd.read_parquet('/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/branch_service_camelcase_values.parquet')
print('Data Loaded...')

#Round of prices to two decimal places
df_branch_service = df_branch_service.round({'price': 2})

## Saving Data
print('Saving Data...')
df_branch_service.to_parquet('/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/branch_service_roundoff_values.parquet')