import pandas as pd
import requests
import motor.motor_asyncio
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, BackgroundTasks
from pymongo import MongoClient, ASCENDING
from pymongo.collection import ReturnDocument
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import os
import datetime
import io

CSV_URL = "https://api.mockaroo.com/api/501b2790?count=100&key=8683a1c0"
DOWNLOADED_FILE_NAME = "downloaded_file.csv"
DB_NAME = "adcore"
COLLECTION_NAME = "courses"
MONGO_URI = "mongodb+srv://hunnyhunnyhunny:voIUd13MmKgKNVPo@cluster0.hqxrzv5.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATA_EXPIRYTIME_IN_MINUTES = 10
INTERVAL_TO_CHECK_DATA_EXPIRY_IN_MINUTES = 1


scheduler = BackgroundScheduler()
scheduler.start()

app = FastAPI()

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Scheduler to check for data expiration and restart the process
scheduler = BackgroundScheduler()
scheduler.start()


def fetch_and_store_data():
    url = CSV_URL
    response = requests.get(url)
    response.raise_for_status()

    # Assume the file is a CSV; if it's another delimited format, adjust accordingly
    delimiter = ","  # Adjust if needed
    data = pd.read_csv(io.StringIO(response.text), delimiter=delimiter)

    # Normalize the data (e.g., handle missing values, etc.)
    normalized_data = data.dropna()  # Example normalization

    # Save to MongoDB with expiration
    expiration_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=10)
    normalized_data_dict = normalized_data.to_dict(orient="records")

    collection.drop()  # Clear previous data
    collection.insert_many(normalized_data_dict)

    # Ensure the data expires after 10 minutes
    collection.create_index(
        "createdAt", expireAfterSeconds=DATA_EXPIRYTIME_IN_MINUTES * 60
    )
    collection.update_many({}, {"$set": {"createdAt": datetime.datetime.utcnow()}})
    print("Data inserted to MongoDb")


def check_data_expiration():
    if collection.count_documents({}) == 0:
        print("data expired!")
        fetch_and_store_data()


scheduler.add_job(
    func=check_data_expiration,
    trigger=IntervalTrigger(minutes=INTERVAL_TO_CHECK_DATA_EXPIRY_IN_MINUTES),
    id="check_expiration_job",
    name="Check data expiration every minute",
    replace_existing=True,
)


@app.on_event("startup")
async def startup_event():
    check_data_expiration()


@app.get("/")
async def root():
    return {"message": "Data fetching and storage service is running."}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
