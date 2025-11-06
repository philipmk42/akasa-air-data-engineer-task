import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER", "akasa")
DB_PASSWORD = os.getenv("DB_PASSWORD", "akasa")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "akasa")
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Asia/Kolkata")

def db_url() -> str:
    return f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
