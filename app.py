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

from typing import List, Optional
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from datetime import datetime as api_datetime
from fastapi.middleware.cors import CORSMiddleware


CSV_URL = "https://api.mockaroo.com/api/501b2790?count=100&key=8683a1c0"
DB_NAME = "adcore"
COLLECTION_NAME = "courses"
MONGO_URI = os.environ.get("MONGO_URI")
PORT = os.environ.get("PORT") or 8000

DATA_EXPIRYTIME_IN_MINUTES = 10
INTERVAL_TO_CHECK_DATA_EXPIRY_IN_MINUTES = 1


class Course(BaseModel):
    University: str
    City: str
    Country: str
    CourseName: str
    CourseDescription: str
    StartDate: api_datetime
    EndDate: api_datetime
    Price: float
    Currency: str


class UpdateCourseModel(BaseModel):
    University: Optional[str] = None
    City: Optional[str] = None
    Country: Optional[str] = None
    CourseName: Optional[str] = None
    CourseDescription: Optional[str] = None
    StartDate: Optional[api_datetime] = None
    EndDate: Optional[api_datetime] = None
    Price: Optional[float] = None
    Currency: Optional[str] = None


def course_serializer(course) -> dict:
    return {
        "id": str(course["_id"]),
        "University": course["University"],
        "City": course["City"],
        "Country": course["Country"],
        "CourseName": course["CourseName"],
        "CourseDescription": course["CourseDescription"],
        "StartDate": course["StartDate"],
        "EndDate": course["EndDate"],
        "Price": course["Price"],
        "Currency": course["Currency"],
    }


scheduler = BackgroundScheduler()
scheduler.start()

app = FastAPI()

origins = [
    "https://adcore-fullstack.netlify.app/",
    "http://adcore-fullstack.netlify.app/",
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    fetch_and_store_data()
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

api_client = AsyncIOMotorClient(MONGO_URI)
api_db = api_client[DB_NAME]
api_collection = api_db[COLLECTION_NAME]


@app.on_event("startup")
async def startup_event():
    check_data_expiration()


@app.get("/")
async def root():
    return {"message": "Data fetching and storage service is running."}


@app.get("/courses/")
async def get_courses(
    University: Optional[str] = None,
    City: Optional[str] = None,
    Country: Optional[str] = None,
    CourseName: Optional[str] = None,
    CourseDescription: Optional[str] = None,
    skip: int = 0,
    limit: int = 999999,
):
    query = {}
    if University:
        query["University"] = {"$regex": University, "$options": "i"}
    if City:
        query["City"] = {"$regex": City, "$options": "i"}
    if Country:
        query["Country"] = {"$regex": Country, "$options": "i"}
    if CourseName:
        query["CourseName"] = {"$regex": CourseName, "$options": "i"}
    if CourseDescription:
        query["CourseDescription"] = {"$regex": CourseDescription, "$options": "i"}

    courses = (
        await api_collection.find(query).skip(skip).limit(limit).to_list(length=limit)
    )
    return [course_serializer(course) for course in courses]


@app.post("/courses/")
async def create_course(course: Course):
    course_data = course.dict()
    result = await api_collection.insert_one(course_data)
    return {"id": str(result.inserted_id)}


@app.put("/courses/{course_id}")
async def update_course(course_id: str, update_data: UpdateCourseModel):
    update_data_dict = update_data.dict(exclude_unset=True)
    result = await api_collection.update_one(
        {"_id": ObjectId(course_id)}, {"$set": update_data_dict}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Course not found")
    return {"status": "success"}


@app.delete("/courses/{course_id}")
async def delete_course(course_id: str):
    result = await api_collection.delete_one({"_id": ObjectId(course_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Course not found")
    return {"status": "success"}
