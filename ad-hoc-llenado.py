import json
import pandas as pd
import numpy as np
import sqlalchemy
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning) 
json_file_path = "/Users/Angebaboy/Documents/Aiane/ad-hoc/transaction-data-adhoc-analysis.json"
with open(json_file_path,'r') as j:
    contents = json.loads(j.read())
    
df = pd.read_json (r'/Users/Angebaboy/Documents/Aiane/ad-hoc/transaction-data-adhoc-analysis.json')

# convert transaction date from string to datetime value
df['transaction_date']= pd.to_datetime(df['transaction_date'])
# extract transaction month and convert to month name
df['Transaction Month'] = pd.to_datetime(df['transaction_date']).dt.month
df['Transaction Month'] = pd.to_datetime(df['Transaction Month'], format='%m').dt.month_name()
df[['Transaction Month','transaction_items','transaction_value']]
# split transaction_items
df[['Item_1', 'Item_2', 'Item_3']] = df['transaction_items'].str.split(';', expand=True)
# split item 1
df[['Item1_Name1', 'Item1_Name2', 'Item1_Q']] = df['Item_1'].str.split(',', expand=True)
# split item 2
df[['Item2_Name1', 'Item2_Name2', 'Item2_Q']] = df['Item_2'].str.split(',', expand=True)
# split item 3
df[['Item3_Name1', 'Item3_Name2', 'Item3_Q']] = df['Item_3'].str.split(',', expand=True)
# concat item 1
df['Item1_Final'] = df['Item1_Name1']+ ', ' + df['Item1_Name2']
# concat item 2
df['Item2_Final'] = df['Item2_Name1']+ ', ' + df['Item2_Name2']
# concat item 3
df['Item3_Final'] = df['Item3_Name1']+ ', ' + df['Item3_Name2']

# convert null to 
df['Item2_Q2'] = df['Item2_Q'].fillna('0')
df['Item3_Q2'] = df['Item3_Q'].fillna('0')

def convert_items (y):
    charset = [*[str(i) for i in range(10)]]
    y = ','.join([i for i in y if i in charset])
    return int(y)

df['Item1_Quantity'] = df['Item1_Q'].apply(convert_items)
df['Item2_Quantity'] = df['Item2_Q2'].apply(convert_items)
df['Item3_Quantity'] = df['Item3_Q2'].apply(convert_items)


df_clean = df[['transaction_date','Transaction Month','Item1_Final','Item1_Quantity','Item2_Final','Item2_Quantity','Item3_Final','Item3_Quantity','transaction_value']]

quantity_per_item_num = df_clean.groupby(['Transaction Month','Item1_Final'],sort=False).agg({'Item1_Quantity':sum,'Item2_Quantity':sum,'Item3_Quantity':sum}).reset_index()
quantity_per_item_num.rename(columns = {'Item1_Final':'Item Name'}, inplace=True)
total_quantity_per_item = ['Item1_Quantity','Item2_Quantity', 'Item3_Quantity']

quantity_per_item_num['Total Quantity per Item'] = quantity_per_item_num[total_quantity_per_item].sum(axis=1)

quantity_per_item_num.drop(['Item1_Quantity','Item2_Quantity', 'Item3_Quantity'], axis=1, inplace=True)

#breakdown of count per item per month 
quantity_per_item_num