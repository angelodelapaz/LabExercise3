import pandas as pd
import numpy as np
from datetime import date
import time

timestr = time.strftime("%Y%m%d-%H%M%S")
df_customer_transaction = pd.read_json("/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/customer_transaction_info.json")
print('Current time: ' + timestr)
print('Data Loaded...')

##Drop Duplicates
print('Dropping Future Dates...')
df_customer_transaction['avail_date'] = pd.to_datetime(df_customer_transaction['avail_date'], errors='coerce')
df_customer_transaction['birthday'] = pd.to_datetime(df_customer_transaction['birthday'], errors='coerce')
df_filt = df_customer_transaction[(df_customer_transaction['avail_date'] <= pd.to_datetime(date.today()))]
df_filt = df_filt[(df_filt['birthday'] <= pd.to_datetime(date.today()))]
print('Dropping Faulty Dates...')
df_bday_avail = df_filt[(df_filt['birthday'] < df_filt['avail_date'])]
print('Dropped Invalid Dates...')

#Save Data
print('Saving Data...')
df_customer_transaction.to_parquet("/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/customer_transaction_faultydates_removed.parquet")

