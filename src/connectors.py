from pymongo import MongoClient
import pandas as pd

def mongo_connect(table_name='BPM_Players'):
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Basketball']  # Replace with your database name
    collection = db[f'{table_name}']  # Replace with your collection name

    # Query the database (retrieve all documents from the collection)
    cursor = collection.find({})



    # Convert to DataFrame
    data = pd.DataFrame(list(cursor))

    # Optionally, drop the '_id' column (automatically added by MongoDB)
    if '_id' in data:
        data.drop('_id', axis=1, inplace=True)
    # Close the connection
    client.close()
    return data

def add_to_mongo(dic,db_name='team_stats'):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Basketball']
    
    

    for record in dic:
        if db_name not in ['team_stats' ,'events_stats','market_bet','market_spreads','BPM_squad']:
            collection_player_stats = db[f'{db_name}']
            collection = collection_player_stats
            query_keys = ['League', 'Season', 'Team', 'Player']
        elif db_name == 'team_stats':
            collection_team_stats = db['team_stats']
            collection = collection_team_stats
            query_keys = ['League', 'Season', 'Team']
        elif db_name == 'events_stats':
            collection_events_stats = db['events_stats']
            collection = collection_events_stats
            query_keys = ['League', 'Season','Event ID']
        elif db_name == 'market_bet':
            collection_market_bet = db['market_bet']
            collection = collection_market_bet
            query_keys = ['league_name','Team']
        elif db_name == 'market_spreads':
            collection_market_spreads = db['market_spreads']
            collection = collection_market_spreads
            query_keys = ['event_id']
        elif db_name == 'BPM_squad':
            collection_BPM_squad = db['BPM_squad']
            collection = collection_BPM_squad
            query_keys = ['League','Team']
        

        # Build the query based on the relevant keys
        query = {key: record[key] for key in query_keys}
        collection.update_one(query, {'$set': record}, upsert=True)

    # Close the connection
    client.close()