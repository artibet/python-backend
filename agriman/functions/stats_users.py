from docx import Document
from io import BytesIO
from fastapi.responses import StreamingResponse

import pandas as pd
from openpyxl import load_workbook
from sqlalchemy import create_engine
from functools import reduce
import numpy as np
import json

def find_parcel_cost(value, parcel_costs):
  for i in range(len(parcel_costs)):
    if value >= parcel_costs[i][0] and value <= parcel_costs[i][1]:
      return(parcel_costs[i][2])
      break

def compute_cost(row, parcel_costs):
    # βασικό κόστος από τη συνάρτηση
    cost = find_parcel_cost(row['num_parcels'], parcel_costs)
    
    # +10 αν num_stables > 0
    if row['num_stables'] > 0:
        cost += 10
    
    # +10 αν num_equals > 0
    if row['num_equals'] > 0:
        cost += 10
    
    return cost


def get_stats_users(period_id):
  host="localhost"      # IP της βάσης στο δίκτυο
  user="root"
  password="Pa$$w0rdBL11"
  database="agriman"
  engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
  query1 = f"""
  SELECT
    applications.afm,
    applications.firstname,
    applications.lastname,
    applications.status_id,
    applications.book_number
  FROM applications
  WHERE applications.period_id = {period_id}
  """
  df1 = pd.read_sql(query1, con=engine)
  ## Μετατροπή book_number σε string και αφαίρεση κενών
  df1['book_number'] = df1['book_number'].astype(str).str.strip()

  ## Αντικατάσταση όλων των book_number που ξεκινούν από 2502 με την τιμή '2502'
  df1['book_number'] = df1['book_number'].apply(lambda x: '2502*' if x.startswith('2502') else x)

  query2 = f"""
  SELECT
    applications.afm,
    COUNT(*) AS num_parcels
  FROM parcels 
  JOIN applications ON applications.id = parcels.application_id
  WHERE applications.period_id = {period_id} AND parcels.is_pasture = 0
  GROUP BY parcels.application_id
  ORDER BY applications.afm, parcels.application_id
  """
  df2 = pd.read_sql(query2, con=engine)

  query3 = f"""
  SELECT
    applications.afm,
    COUNT(*) AS num_stables
  FROM stables 
  JOIN applications ON applications.id = stables.application_id
  WHERE applications.period_id = {period_id} 
  GROUP BY stables.application_id
  ORDER BY applications.afm, stables.application_id
  """
  df3 = pd.read_sql(query3, con=engine)

  query4 = f"""
  SELECT
    applications.afm,
    COUNT(*) AS num_equals
  FROM application_measures 
  JOIN applications ON applications.id = application_measures.application_id
  WHERE applications.period_id = {period_id} AND application_measures.code IN ('13.1','13.2','13.3')
  GROUP BY application_measures.application_id
  ORDER BY applications.afm, application_measures.application_id
  """
  df4 = pd.read_sql(query4, con=engine)

  query5 = f"""
  SELECT
    parcel_costs.from_parcels,
    parcel_costs.to_parcels,
    parcel_costs.cost,
    periods.year AS year
  FROM parcel_costs
  JOIN periods ON parcel_costs.period_id=periods.id
  WHERE periods.id = {period_id}
  ORDER BY parcel_costs.from_parcels
  """
  df5 = pd.read_sql(query5, con=engine)
  cols=['from_parcels','to_parcels','cost']
  parcel_costs = df5[cols].values.tolist()

# Βάζεις όλα τα df σε λίστα
  dfs = [df1, df2, df3, df4]

# Κάνεις merge με outer join πάνω στο 'afm'
  df_final = reduce(lambda left, right: pd.merge(left, right, on="afm", how="outer"), dfs)

  df_final = df_final.fillna(0)

## Εφαρμογή στο df
  df_final['final_cost'] = df_final.apply(lambda row: compute_cost(row, parcel_costs), axis=1)

## Προαιρετικά: καθάρισε NaN σε αριθμητικές στήλες για να βγουν σωστά τα sums
  num_cols = ['num_parcels', 'num_stables', 'num_equals', 'final_cost']
  df_final[num_cols] = df_final[num_cols].apply(pd.to_numeric, errors='coerce').fillna(0)

  grouped = (
    df_final.groupby('book_number')
      .agg(
        num_parcels_sum = ('num_parcels', 'sum'),
        num_stables_sum = ('num_stables', 'sum'),
        num_equals_sum  = ('num_equals',  'sum'),
        final_cost_sum  = ('final_cost',  'sum'),
        afm_count       = ('afm', 'nunique'),                 # πλήθος μοναδικών AFM
        status_0        = ('status_id', lambda s: (s==0).sum()),
        status_1        = ('status_id', lambda s: (s==1).sum())
      )
      .reset_index()
  )

  stats = grouped.to_json(orient="records", force_ascii=False)

  # Μετατροπή σε Python object (λίστα από dicts)
  stats = json.loads(stats)

##  
##  # Create a sample stats array of dictionaries 
##  stats = [
##    {
##      'book_number': 100,
##      'afm_count': 131,
##      'proxeires': 130,
##      'oristikes': 1,
##      'parcel_count': 2730,
##      'cattles': 716,
##      'animals_total': 1466
##    },
##    {
##      'book_number': 1014,
##      'afm_count': 42,
##      'proxeires': 42,
##      'oristikes': 0,
##      'parcel_count': 579,
##      'cattles': 0,
##      'animals_total': 100
##    },
##    {
##      'book_number': 104,
##      'afm_count': 312,
##      'proxeires': 308,
##      'oristikes': 4,
##      'parcel_count': 6254,
##      'cattles': 2499,
##      'animals_total': 33212
##    },
##    
##  ]

  # Return stats as json 
  return stats
