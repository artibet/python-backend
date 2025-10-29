from sqlalchemy import create_engine
from dotenv import load_dotenv
import math
import numpy as np
import os

# load .env file
load_dotenv()

DB_USER = os.getenv("KATANOMI_DB_USER")
DB_PASS = os.getenv("KATANOMI_DB_PASS")
DB_HOST = os.getenv("KATANOMI_DB_HOST", "127.0.0.1")
DB_NAME = os.getenv("KATANOMI_DB_NAME")

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

'''
Return the created database engine
'''
def get_engine():
  return engine

'''
Convert NaN numpy values to None
'''
def sql_safe(value):
  if value is None:
      return None
  if isinstance(value, float) and math.isnan(value):
      return None
  if isinstance(value, (np.floating, np.integer)) and np.isnan(value):
      return None
  return value

