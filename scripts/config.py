import os
from dotenv import load_dotenv

load_dotenv()

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "dbname": os.getenv("PG_DATABASE", "call_center"),
    "user": os.getenv("PG_USER", "admin"),
    "password": os.getenv("PG_PASSWORD", "admin"),
}

CH_CONFIG = {
    "host": os.getenv("CH_HOST", "localhost"),
    "port": int(os.getenv("CH_PORT", 8123)),
    "database": os.getenv("CH_DATABASE", "default"),
    "username": os.getenv("CH_USER", "default"),
    "password": os.getenv("CH_PASSWORD", ""),
}