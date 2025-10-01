from sqlalchemy import create_engine
from dotenv import load_dotenv
from sqlalchemy import text
import pandas as pd
import math
import numpy as np
import os

# load .env file
load_dotenv()

DB_USER = os.getenv("AGRIMAN_DB_USER")
DB_PASS = os.getenv("AGRIMAN_DB_PASS")
DB_HOST = os.getenv("AGRIMAN_DB_HOST", "127.0.0.1")
DB_NAME = os.getenv("AGRIMAN_DB_NAME")

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

'''
Return the created database engine
'''
def get_engine():
  return engine

'''
Update application_checks table
'''
def update_application_checks(app_id, tag, passed, notes):

  # Get check_id from tag
  query = text("""
    SELECT  id
    FROM checks
    WHERE tag = :tag
  """)
  df=pd.read_sql(query, con=engine, params={'tag': tag})
  if df.empty:
    print(f"No check with tag {tag}")
    return
  else:
    check_id = df['id'].iloc[0]

  # Update
  with engine.begin() as conn:
    conn.execute(text("""
      UPDATE application_checks
      SET checked_at = UTC_TIMESTAMP(),
      passed = :passed,
      notes = :notes
      WHERE application_id = :app_id AND check_id = :check_id
    """), {'passed': passed, 'notes': notes, 'app_id': app_id, 'check_id': check_id})

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

