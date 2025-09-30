from sqlalchemy import text
import pandas as pd
from agriman.database import get_engine, update_application_checks

def check_crop_connected(id_key):
	tag = 'crop_connected'
	engine = get_engine()
	
	query1 = text("""
    SELECT
      applications.afm,
      applications.year,
      support_schemes.code AS code,
      parcel_cultivations.area,
      parcels.code AS aa,
      parcel_cultivations.id,
      parcel_cultivations.variety_id,
      supports.code AS scode
    FROM applications
    JOIN parcels ON applications.id = parcels.application_id
    JOIN parcel_cultivations ON parcels.id = parcel_cultivations.parcel_id
    JOIN support_varieties ON support_varieties.variety_id = parcel_cultivations.variety_id
    JOIN supports ON supports.id = support_varieties.support_id
    LEFT OUTER JOIN support_schemes ON support_schemes.parcel_cultivation_id = parcel_cultivations.id
    WHERE applications.id = :app_id AND supports.period_id = applications.period_id 
    AND supports.code IN ('0101','0102','0103','0104','0105','0106','0107','0109','0112','0113','0114','0118','0120','0122','0123','0124','0125','0126','0127','0323','0501')
    ORDER BY parcel_cultivations.id
  """)
	df1=pd.read_sql(query1, con=engine, params={'app_id': id_key})
	if len(df1) == 0:
		status_1 = -1
	else:
		# df1=df1[df1['scode'] != '1101']
		df_con=df1[df1['code'] != df1['scode']]
		if len(df_con) == 0:
			status_1 = 1
		else:
			status_1 = 0
			res_list=df_con.values.tolist()
			msmall=[]
			for row in res_list:
				msm=f'A/A {row[4]}: Δηλωμένη Συνδεδεμένη: {row[2]} και υποστηριζόμενη: {row[7]}'
				msmall.append(msm)

	if status_1 == -1:
		passed = 1
		notes='Δεν έχουν δηλωθεί Συνδεδεμένες'
	else:
		if status_1 == 1:
			passed = 1
			notes='Οι δηλωμένες Συνδεδεμένες είναι σωστές'
		else:
			passed = 0
			notes = "\n".join(msmall)

	update_application_checks(id_key, tag, passed, notes)
	