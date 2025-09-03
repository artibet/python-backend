from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# load .env file
load_dotenv()

DB_USER = os.getenv("AGRIMAN_DB_USER")
DB_PASS = os.getenv("AGRIMAN_DB_PASS")
DB_HOST = os.getenv("AGRIMAN_DB_HOST", "127.0.0.1")
DB_NAME = os.getenv("AGRIMAN_DB_NAME")

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

def get_engine():
  return engine
