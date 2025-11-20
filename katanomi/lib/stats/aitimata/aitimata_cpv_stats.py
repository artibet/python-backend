from sqlalchemy import text

import pandas as pd
from katanomi.database import get_engine
import json


# def find_parcel_cost(value, parcel_costs):
#   for i in range(len(parcel_costs)):
#     if value >= parcel_costs[i][0] and value <= parcel_costs[i][1]:
#       return(parcel_costs[i][2])
#   return 0

# def compute_cost(row, parcel_costs):
#     # βασικό κόστος από τη συνάρτηση
#     cost = find_parcel_cost(row['num_parcels'], parcel_costs)
    
#     # +10 αν num_stables > 0
#     if row['num_stables'] > 0:
#         cost += 10
    
#     # +10 αν num_equals > 0
#     if row['num_equals'] > 0:
#         cost += 10
    
#     return cost


def get_aitimata_cpv_stats(period_id):
  engine = get_engine()
  query0 = text("""
  SELECT
    periods.descr
  FROM periods
  WHERE periods.id = :period_id
  """)
  df0=pd.read_sql(query0, con=engine, params={'period_id': period_id})

  query1 = text("""
  SELECT
    aitimata.period_id,
    aitimata.status_id,
    aitimata.method_id,
    aitimata.arithmos,
    aitimata.etos,
    arecs.admin_cpv_id,
    arecs.kae_id,
    arecs.net_total_cost,
    arecs.in_cpv_sum,
    cpvs.code as cpv_code,
    cpvs.descr as cpv_descr            
  FROM aitimata 
  JOIN arecs ON arecs.aitima_id = aitimata.id
  JOIN cpvs ON cpvs.id = arecs.admin_cpv_id
  WHERE aitimata.period_id = :period_id 
  AND aitimata.status_id > 100 
  AND arecs.in_cpv_sum = 1
  """)

  df1=pd.read_sql(query1, con=engine, params={'period_id': period_id})

  query2 = text("""
  SELECT
    adeps.period_id,
    commitments.status_id,
    commitments.admin_cpv_id,
    commitments.kae_id,
    commitments.net_total_cost,
    cpvs.code as cpv_code,
    cpvs.descr as cpv_descr            
  FROM adeps 
  JOIN commitments ON adeps.id = commitments.adep_id
  JOIN cpvs ON cpvs.id = commitments.admin_cpv_id
  WHERE adeps.period_id = :period_id 
  AND commitments.status_id in (2,3,4)
  """)

  df2=pd.read_sql(query2, con=engine, params={'period_id': period_id})  

  # group df1
  grouped1 = (
    df1.groupby(['cpv_code', 'cpv_descr'])
      .agg(
        total_aa = (
          'net_total_cost',
          lambda s: s[
              (df1.loc[s.index, 'status_id'] >= 101)
              & (df1.loc[s.index, 'status_id'] < 200)
          ].sum()
        ),

       total_diag = ('net_total_cost', lambda s: s[df1.loc[s.index, 'status_id'] == 201].sum()),
       total_other = ('net_total_cost', lambda s: s[df1.loc[s.index, 'status_id'] == 301].sum()),
       total_rejected = ('net_total_cost', lambda s: s[df1.loc[s.index, 'status_id'] == 401].sum())
      )
      .reset_index()
  )

  # group df2
  grouped2 = (
    df2.groupby(['cpv_code', 'cpv_descr'])
      .agg(
       total_aa = ('net_total_cost', lambda _: 0),
       total_diag = ('net_total_cost', lambda s: s[df2.loc[s.index, 'status_id'] == 2].sum()),
       total_other = ('net_total_cost', lambda s: s[df2.loc[s.index, 'status_id'] == 3].sum()),
       total_rejected = ('net_total_cost', lambda s: s[df2.loc[s.index, 'status_id'] == 4].sum())
      )
      .reset_index()
  )

  # merge
  merged = (
    grouped1
    .merge(grouped2, on=['cpv_code', 'cpv_descr'], how='outer', suffixes=('_g1', '_g2'))
  )
  merged = merged.fillna(0)
  merged['total_aa'] = merged['total_aa_g1'] + merged['total_aa_g2']
  merged['total_diag'] = merged['total_diag_g1'] + merged['total_diag_g2']
  merged['total_other'] = merged['total_other_g1'] + merged['total_other_g2']
  merged['total_rejected'] = merged['total_rejected_g1'] + merged['total_rejected_g2']
  merged = merged.drop(columns=['total_aa_g1', 'total_diag_g1', 'total_other_g1', 'total_rejected_g1','total_aa_g2', 'total_diag_g2', 'total_other_g2', 'total_rejected_g2'])
  
  stats = merged.to_json(orient="records", force_ascii=False)

  # Μετατροπή σε Python object (λίστα από dicts)
  stats = json.loads(stats)

  # # Return stats as json 
  return stats
