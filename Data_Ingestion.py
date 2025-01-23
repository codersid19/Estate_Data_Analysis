from pymongo import MongoClient
import pandas as pd

def check_mongo():
    
    uri = "mongodb+srv://siddhant19shirodkar12:Admin19@cluster0.7cc2g.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    # Create a new client and connect to the server
    client = MongoClient(uri)
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)



def fetch_data_from_mongodb():
    """Fetch data from MongoDB and save it to a CSV file."""
    try:
        check_mongo()
        # Replace <username>, <password>, and <cluster-url> with your MongoDB Atlas credentials
        mongo_uri = "mongodb+srv://siddhant19shirodkar12:Admin19@cluster0.7cc2g.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

        # Connect to MongoDB Atlas
        client = MongoClient(mongo_uri)

        # Specify the database and collection
        db = client['real_estate_db']
        collection = db['properties']

        # Fetch all records from the collection
        data = list(collection.find())

        # Convert MongoDB records to a DataFrame
        df = pd.DataFrame(data)

        # Drop the MongoDB-specific `_id` field if it exists
        if '_id' in df.columns:
            df.drop(columns=['_id'], inplace=True)

        # Save the DataFrame to a CSV file
        csv_file_path = 'data/real_estate_data.csv'
        df.to_csv(csv_file_path, index=False)

        print(f"Data successfully saved to {csv_file_path}")

    except Exception as e:
        print(f"An error occurred: {e}")

# Run the function
fetch_data_from_mongodb()
