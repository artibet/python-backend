from sqlalchemy import text
import pandas as pd
from agriman.database import get_engine, update_application_checks

def check_application_ecoschemes(id_key):
	tag = 'application_ecoschemes'
	engine = get_engine()
	
	query1 = text("""
		SELECT
			applications.afm,
			applications.year,
			ecoschemes.code AS code
		FROM parcels
		JOIN applications ON applications.id = parcels.application_id
		JOIN ecoschemes ON ecoschemes.parcel_id = parcels.id
		WHERE applications.id = :app_id AND parcels.is_seed_cultivation = 0
		GROUP BY applications.afm, ecoschemes.code
		ORDER BY ecoschemes.code
	""")
	df1=pd.read_sql(query1, con=engine, params={'app_id': id_key})

	if len(df1) == 0:
		status_1 = 0
	else:
		status_1 = 1
		query2 = text("""
			SELECT
				applications.afm,
				application_ecoschemes.code
			FROM applications 
			JOIN application_ecoschemes ON application_ecoschemes.application_id = applications.id
			WHERE applications.id = :app_id 
			ORDER BY application_ecoschemes.code
		""")
		df2=pd.read_sql(query2, con=engine, params={'app_id': id_key})
		if len(df2) == 0:
			status_2 = 0
		else:
			status_2 = 1
			# query3 = f"""
			# SELECT
			# 	corresponds.target,
			# FROM corresponds 
			# WHERE corresponds.scope = ecoscheme AND corresponds.source = '{eco}' 
			# ORDER BY application_ecoschemes.code
			# """
			# df3=pd.read_sql(query3, con=engine)


	passed=None
	notes=''
	if status_1 == 0:
		passed = 1
		notes='Δεν έχουν δηλωθεί Οικοσχήματα'
	else:
		codes_str = ", ".join(df1["code"].tolist())
		if status_2 == 0:
			passed = 0
			notes='Έχουν δηλωθεί τα Οικοσχήματα: '+ codes_str +' δεν υπάρχουν στην δήλωση'
		else:
			passed = 0
			codes_str2 = ", ".join(df2["code"].tolist())
			notes='Έχουν δηλωθεί τα Οικοσχήματα: '+ codes_str +' και υπάρχουν στην δήλωση: '+ codes_str2

	update_application_checks(id_key, tag, passed, notes)
