from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Basketball']  # Replace 'yourDatabaseName' with your actual database name
collection = db['BPM_Player']  # Replace 'yourCollectionName' with your actual collection name

# Update operation to unset fields
collection.update_many(
    {},  # Filter for the documents; an empty dictionary {} matches all documents in the collection.
    {
        '$unset': {
            'trader rating': '',  # The value is ignored, just need to specify the field name
            'Missing': '',

        }
    }
)