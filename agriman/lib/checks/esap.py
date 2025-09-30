from sqlalchemy import text
import pandas as pd
from agriman.database import get_engine, update_application_checks

def check_esap(id_key):
	tag = 'esap'
	engine = get_engine()
		
	query1 = text("""
	SELECT
	    applications.esap
	FROM applications 
	WHERE applications.id = :id 
	""")
	df1=pd.read_sql(query1, con=engine, params={'id': id_key})
	if df1.empty:
		status_1 = 0
	else:
		val = df1.loc[0, 'esap']
		if pd.isna(val) or val == "" or val is None:
			status_1 = 0
		else:
			status_1 = 1

	query2 = text("""
	SELECT
	    applications.afm,
	    ecoschemes.code
	FROM applications 
	JOIN parcels ON parcels.application_id = applications.id
	JOIN ecoschemes ON ecoschemes.parcel_id=parcels.id
	WHERE applications.id = :id AND ecoschemes.code LIKE :eco_pattern
    ORDER BY ecoschemes.code
	""")
	df2=pd.read_sql(query2, con=engine, params={'id': id_key, 'eco_pattern': 'ECO-06.%'})
	if len(df2) == 0:
		status_2 = 0
	else:
		status_2 = 1

	if status_1 == 0 and status_2 == 0:
		passed = 1
		notes='Δεν έχει δηλωθεί Ψηφιακή Εφαρμογή και δεν έχει δηλωθεί Οικοσχήμα της μορφής ECO-06.*'
	if status_1 == 1 and status_2 == 1:
		passed = 1
		notes='Έχει δηλωθεί Ψηφιακή Εφαρμογή και έχει δηλωθεί Οικοσχήμα της μορφής ECO-06.*'
	if status_1 == 1 and status_2 == 0:
		passed = 0
		notes='Έχει δηλωθεί Ψηφιακή Εφαρμογή και δεν έχει δηλωθεί Οικοσχήμα της μορφής ECO-06.*'
	if status_1 == 0 and status_2 == 1:
		passed = 0
		notes='Δεν έχει δηλωθεί Ψηφιακή Εφαρμογή και έχει δηλωθεί Οικοσχήμα της μορφής ECO-06.*'

	update_application_checks(id_key, tag, passed, notes)
	