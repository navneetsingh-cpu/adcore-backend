import pandas as pd
import requests
import motor.motor_asyncio
from pymongo import MongoClient
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler

CSV_URL = "https://api.mockaroo.com/api/501b2790?count=100&key=8683a1c0"
DOWNLOADED_FILE_NAME = "downloaded_file.csv"
DATABASE_NAME = "adcore"
COLLECTION_NAME = "courses"
MONGO_URL = "mongodb+srv://hunnyhunnyhunny:voIUd13MmKgKNVPo@cluster0.hqxrzv5.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"




def download_csv(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, "wb") as file:
            file.write(response.content)
        print(f"Downloaded and saved {filename}")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")


def normalize_Using_pandas():
    df = pd.read_csv(DOWNLOADED_FILE_NAME)
    df["StartDate"] = pd.to_datetime(df["StartDate"])
    df["EndDate"] = pd.to_datetime(df["EndDate"])

    # Normalize text data: Convert CourseName and CourseDescription to lower case
    df["CourseName"] = df["CourseName"].str.lower()
    df["CourseDescription"] = df["CourseDescription"].str.lower()

    return df


async def save_dataframe_to_mongo(df, db_name, collection_name, mongo_uri=MONGO_URL):
    # Asynchronous MongoDB client
    client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Convert DataFrame to dictionary
    data_dict = df.to_dict("records")

    # Insert data into MongoDB
    result = await collection.insert_many(data_dict)
    print(
        f"Inserted {len(result.inserted_ids)} documents into {collection_name} collection."
    )


scheduler = BackgroundScheduler()
scheduler.start()

# 1. Download the delimited file from the provided link.
download_csv(CSV_URL, DOWNLOADED_FILE_NAME)

# 2. Normalize it in memory using Pandas.
data = normalize_Using_pandas()

# 3. Save the normalized data into MongoDB.
asyncio.run(save_dataframe_to_mongo(data, DATABASE_NAME, COLLECTION_NAME))

# 4. Ensure the data in MongoDB expires every 10 minutes.

# 5. If the data expires, start from step one
