# config.py
from dotenv import load_dotenv
import os

load_dotenv()  # Charge le fichier .env

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME")
}

TABLE_NAME = os.getenv("TABLE_NAME", "call_logs")
