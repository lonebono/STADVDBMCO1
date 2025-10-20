import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

LOCAL_DB = os.getenv("LOCAL_DB")
LOCAL_USER = os.getenv("LOCAL_USER")
LOCAL_PASSWORD = os.getenv("LOCAL_PASSWORD")
LOCAL_HOST = os.getenv("LOCAL_HOST")
LOCAL_PORT = os.getenv("LOCAL_PORT")
DATABASE_URL = os.getenv("DATABASE_URL")

local_engine = create_engine(
    f"postgresql+psycopg2://{LOCAL_USER}:{LOCAL_PASSWORD}@{LOCAL_HOST}:{LOCAL_PORT}/{LOCAL_DB}"
)
supabase_engine = create_engine(DATABASE_URL)
