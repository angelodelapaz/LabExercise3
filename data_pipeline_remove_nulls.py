import pandas as pd
import numpy as np

df_branch_service =  pd.read_parquet('branch_service_duplicates_removed.parquet')
print('Data Loaded...')

## Drop Null Values
print('Dropping Null Values...')
df_branch_service = df_branch_service.dropna(subset=['branch_name'])
df_branch_service = df_branch_service.dropna(subset=['service'])
print('Null Values Dropped Currently at: ', df_branch_service.shape)

## Drop Missing Values 
df_branch_service = df_branch_service.drop(df_branch_service[df_branch_service['branch_name'] == ''].index)
df_branch_service = df_branch_service.drop(df_branch_service[df_branch_service['branch_name'] == 'N/A'].index)

# conditions to fill in missing prices
serviceArray = ['Manicure', 'HairColor', 'FootSpa', 'Rebond', 'Haircut', 'NailColor', 'Pedicure']
priceArray = [55.23, 88.09, 100.12, 400.23, 66.12, 30.12, 77.99]

for i in range(len(serviceArray)):
    df_branch_service.loc[(df_branch_service['service'] == serviceArray[i]) & (df_branch_service['price'].isnull()), 'price'] = priceArray[i]


print('Missing Values Dropped Currently at: ', df_branch_service.shape)


## Saving Data
print('Saving Data...')
df_branch_service.to_parquet('branch_service_nullbranch_removed.parquet')