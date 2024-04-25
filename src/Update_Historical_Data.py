from pymongo  import MongoClient
from datetime import datetime
from connectors import mongo_connect,add_to_mongo
import requests
from pymongo import MongoClient
from datetime import datetime
import pandas as pd


TELEGRAM_BOT_TOKEN = '6313757405:AAESPj-GRvysErDK9Q6wMLg5nOjMk83z8TI'
TELEGRAM_CHAT_ID = '1447321557'


def send_telegram_message(message):
    """Sends a message to a Telegram chat."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    requests.post(url, data=data)

def mongo_db(collection_name):
    client = MongoClient("mongodb://localhost:27017/")  # Adjust the connection string as needed
    db = client["Historical_Data"]  # Replace with your actual database name
    return db[collection_name]

def add_to_historical(source_collection_name, historical_collection_name):
    # Connect to the source collection
    source_collection = mongo_connect(source_collection_name)
    if source_collection_name == 'team_stats':
        df = pd.DataFrame(source_collection)
        df = df[['League','Season','Team','Attacking Strength','Defensive Strength','Pace_league','Sup Rating']]
    else:
        # Convert documents to DataFrame for easier manipulation
        df = pd.DataFrame(source_collection)
    
    # Determine the current week number and format the date
    current_date = datetime.now()
    formatted_date = f"{current_date.strftime('%Y-%m')}-W{(current_date.day) // 7 + 1}"
    df['Week'] = formatted_date
    
    # Convert the DataFrame back to a list of dictionaries
    records = df.to_dict("records")
    
    # Connect to the historical collection
    historical_collection = mongo_db(historical_collection_name)
    
    # Insert the records into the historical collection
    historical_collection.insert_many(records)

# Example usage
try:
    add_to_historical("market_bet", "market_bet_his")
    add_to_historical("BPM_Player", "BPM_Player_his")
    add_to_historical("BPM_squad", "BPM_squad_his")
    add_to_historical("team_stats", "Teams_his")
    send_telegram_message(f"Update_Historical_Data: Ok")


except Exception as e:
    send_telegram_message(f"Update_Historical_Data: {e}")
