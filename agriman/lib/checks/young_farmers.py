from sqlalchemy import text
import pandas as pd
from datetime import date
from agriman.database import get_engine, update_application_checks

def check_young_farmers(id_key):
	tag = 'young_farmers'
	engine = get_engine()
	
	query1 = text("""
	SELECT
		applications.first_submission,
		applications.birth_date
	FROM applications 
	WHERE applications.id = :app_id and (first_submission IS NOT null AND birth_date IS NOT null) 
	""")
	df1=pd.read_sql(query1, con=engine, params={'app_id': id_key})

	if len(df1)==0:
		status_1 = -1
	else:
		df1['birth_date']=pd.to_datetime(df1['birth_date'])
		val=df1.loc[0,'birth_date']
		age=date.today().year - val.year
		if age <= 44:
			status_1 = 1
		else:
			status_1 = 0
	
		val2=df1.loc[0,'first_submission']
		if 2025-val2<=4: 
			status_2 = 1
		else:
			status_2 = 0
	
	query2 = text("""
		SELECT
				applications.afm,
				application_support_schemes.code
		FROM applications 
		JOIN application_support_schemes ON application_support_schemes.application_id = applications.id
		WHERE applications.id = :app_id  AND application_support_schemes.code = '0201'
			ORDER BY application_support_schemes.code
	""")
	df2=pd.read_sql(query2, con=engine, params={'app_id': id_key})
	if len(df2)==0:
		status_3 = 0
	else:
		status_3 = 1

	passed=None
	notes=''
	if status_1 == -1:
		passed = 1
		notes='Δεν έχει δηλώσει δεδομένα σχετικά με ηλικία και πρώτη υποβολή'
	elif status_1 == 0:
		passed = 0
		notes='Ηλικία > 44'
	elif status_1 == 1:
		if status_2 == 1:
			if status_3 == 1:
				passed = 1
				notes='Γεωργός Μικρής Ηλικίας με 0201'
			else:
				passed = 0
				notes='Γεωργός Μικρής Ηλικίας χωρίς 0201'
		else:
			if status_3 == 1:
				passed = 0
				notes='Γεωργός Μικρής Ηλικίας με 0201 με πρώτη δήλωση'+ str(val2)
			else:
				passed = 0
				notes='Γεωργός Μικρής Ηλικίας χωρίς 0201 πρώτη δήλωση'+ str(val2)
	else:
		print("OK")
		
	update_application_checks(id_key, tag, passed, notes)



