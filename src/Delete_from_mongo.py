from pymongo import MongoClient
from connectors import mongo_connect,add_to_mongo
import pandas as pd

# Connect to MongoDB
# client = MongoClient('mongodb://localhost:27017/')
# db = client['Basketball']  # Replace 'yourDatabaseName' with your actual database name
# collection = db['events_stats']  # Replace 'yourCollectionName' with your actual collection name

# Update operation to unset fields
# collection.update_many(
#     {},  # Filter for the documents; an empty dictionary {} matches all documents in the collection.
#     {
#         '$unset': {
#             'home odds to beat handicap_y': ''  # The value is ignored, just need to specify the field name

#         }
#     }
# )

# BPM_Player= mongo_connect('BPM_Player')

# def get_teams_by_league(group):
#     return pd.DataFrame({'Team': group['Team'].unique()})

# result = BPM_Player.groupby('League').apply(get_teams_by_league)
# result.reset_index(level=0, inplace=True)  # Convert League from index to a column 

# market_bet_his = mongo_connect_his('market_bet_his')


# client = MongoClient('mongodb://localhost:27017/')
# db = client['Historical_Data']
# collection = db['market_bet_his']


# for index, row in result.iterrows():
#     team = row['Team']
#     league = row['League']
#     for index, row in market_bet_his.iterrows():
#         league2 = row['league_name']
#         if league2 != league:
#             collection.delete_one({'Team': team, 'league_name': league2})
# client.close()


# events_stats= mongo_connect('events_stats')


# df = events_stats[events_stats['Prediction_Away_BPM']==0]



client = MongoClient('mongodb://localhost:27017/')
db = client['Basketball']
collection = db['events_stats']
df= mongo_connect('events_stats')

for index, row in df.iterrows():
    id = row['Event ID']
    if row['League'] =='Turkish-BSL':
        collection.delete_one({'Event ID': id})
client.close()