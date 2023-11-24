import pandas as pd
import numpy as np
from datetime import date

df_customer_transaction = pd.read_json("customer_transaction_info.json")
print('Data Loaded...')

##Drop Duplicates
print('Dropping Duplicates...')
df_customer_transaction['avail_date'] = pd.to_datetime(df_customer_transaction['avail_date'], errors='coerce')
df_customer_transaction['birthday'] = pd.to_datetime(df_customer_transaction['birthday'], errors='coerce')
df_filt = df_customer_transaction[(df_customer_transaction['avail_date'] <= pd.to_datetime(date.today()))]
df_filt = df_filt[(df_filt['birthday'] <= pd.to_datetime(date.today()))]
df_bday_avail = df_filt[(df_filt['birthday'] < df_filt['avail_date'])]
print('Dropped Duplicates...')

#Save Data
print('Saving Data...')
df_customer_transaction.to_parquet('customer_transaction_duplicates_removed.parquet')
