from sqlalchemy import text
import pandas as pd
from agriman.database import get_engine, update_application_checks

def check_national_reserve(id_key):
	tag = 'national_reserve'
	engine = get_engine()
	
	query1 = text("""
	SELECT
	    applications.first_submission
	FROM applications 
	WHERE applications.id = :app_id  AND applications.first_submission = '2025'
	""")
	df1=pd.read_sql(query1, con=engine, params={'app_id': id_key})
	if len(df1)==0:
		status_1 = 0
	else:
		status_1 = 1

	query2 = text("""
	SELECT
	    applications.afm,
	    application_support_schemes.code
	FROM applications 
	JOIN application_support_schemes ON application_support_schemes.application_id = applications.id
	WHERE applications.id = :app_id  AND application_support_schemes.code = '0402'
    ORDER BY application_support_schemes.code
	""")
	df2=pd.read_sql(query2, con=engine, params={'app_id': id_key})
	if len(df2)==0:
		status_2 = 0
	else:
		status_2 = 1

	if status_1 == 0 and status_2 == 0:
		passed = 1
		notes='Δεν έχει πρώτη δήλωση το 2025 και δεν έχει δηλωθεί 0402'
	if status_1 == 1 and status_2 == 1:
		passed = 1
		notes='Έχει πρώτη δήλωση το 2025 και έχει δηλωθεί 0402'
	if status_1 == 1 and status_2 == 0:
		passed = 0
		notes='Έχει πρώτη δήλωση το 2025 και δεν έχει δηλωθεί 0402'
	if status_1 == 0 and status_2 == 1:
		passed = 0
		notes='Δεν έχει πρώτη δήλωση το 2025 και έχει δηλωθεί 0402'

	update_application_checks(id_key, tag, passed, notes)
	