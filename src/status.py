import pandas as pd
import numpy as np
from pymongo import MongoClient
from connectors import mongo_connect,add_to_mongo
from datetime import datetime

try:
    events= mongo_connect('events_stats')

    status = {
        "Event ID":[],
        'Updated':[],
        'freeze':[]
    }

    for index, row in events.iterrows():
        if 'freeze' in row and row['freeze'] == True:
            status['Updated'].append(row['Updated'])#row['Updated']
            status['freeze'].append(row['freeze'])
            status["Event ID"].append(row['Event ID'])
            continue
        else:
            if row['Status'] == 'Finished':
                status['freeze'].append(True)
                status['Updated'].append(datetime.now())
                
            else:
                status['freeze'].append(False)
                status['Updated'].append(datetime.now())

            status["Event ID"].append(row['Event ID'])

    status = pd.DataFrame(status)
    events = events.merge(status,on="Event ID", how='left')


    column = 'Updated_y'
    if column in events.columns:
        events.drop(['Updated_x','freeze_x'], axis=1,inplace=True)
        # print(events.info())
        new_column_names = {'Updated_y':'Updated','freeze_y':'freeze'}
        events = events.rename(columns=new_column_names)

    events_stats = events
    data_dict_events_stats = events_stats.to_dict("records")
    add_to_mongo(data_dict_events_stats,'events_stats')             
    print("Ok")
except Exception as e:
    print(f"Error: {e}")  # Print the error message if an exception occurs