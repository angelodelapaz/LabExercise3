import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine,VARCHAR, DATE
import time
timestr = time.strftime("%Y%m%d-%H%M%S")
print('Current time: ' + timestr)
#Import Data
print('Reading Data..')
df_branch_service = pd.read_parquet('/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/branch_service_formatted_values.parquet')
df_customer_transaction = pd.read_parquet('/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/finalcustomerinfo.parquet')

#Merge Data
df_merged = pd.merge(df_customer_transaction, df_branch_service)
print('Datasets Merged...')

#Save Data
print('Saving Data...')
df_merged.to_parquet('/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/merged_data.parquet')
print('Saving Weekly Views...')
df_merged['week'] = df_merged['avail_date'].apply(
    lambda date: datetime.date.isocalendar(date)[1])
df_merged.groupby([df_merged['week'], 'service'])['price'].sum().to_frame()
df_merged.to_parquet('/mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/parquets/weekly_view.parquet')

print('Ingesting to Database...')
engine = create_engine('sqlite:////mnt/c/Users/Shaun Padrejuan/Documents/GitHub/LabExercise3/db/german_tips.db', echo=True)
df_merged.to_sql('transactions', con=engine, if_exists='replace', index=False,
                               dtype={
                                   "txn_id": VARCHAR(10),
                                   "avail_date": DATE,
                                   "last_name": VARCHAR(50),
                                   "first_name": VARCHAR(50),
                                   "birthday": DATE,
                                   "branch_name" : VARCHAR(20),
                                   "service" : VARCHAR(20),
                                   "price" : VARCHAR(20),
                               })
