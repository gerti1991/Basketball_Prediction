import pymongo

# Assuming you have a MongoClient instance
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Assuming your database name is "your_database" and your collection name is "your_collection"
db = client.Basketball
collection = db.events_stats

# List of IDs you provided
ids_to_update = ['KYEQbe8M', 'jgN92w9c']

# Update documents where _id is in the list
collection.update_many(
    {'Event ID': {'$in': ids_to_update}},
    {'$set': {'freeze': False}}
)

print("Documents updated successfully.")
