from sqlalchemy import text
import pandas as pd
from agriman.database import get_engine, update_application_checks

def check_pasture_mmz(id_key):
	tag = 'pasture_mmz'
	engine = get_engine()
	
	query1 = text("""
	SELECT
	    applications.mmz_total
	FROM applications 
	WHERE applications.id = :app_id 
	""")
	df1=pd.read_sql(query1, con=engine, params={'app_id': id_key})
	if len(df1)==0:
		status_1 = -1
	else:
		val = df1.loc[0, 'mmz_total']
		if val < 3:
			status_1 = 0
		else:
			status_1 = 1

	query2 = text("""
	SELECT
	    applications.afm,
	    application_support_schemes.code
	FROM applications 
	JOIN application_support_schemes ON application_support_schemes.application_id = applications.id
	WHERE applications.id = :app_id  AND application_support_schemes.code = '0407'
    ORDER BY application_support_schemes.code
	""")
	df2=pd.read_sql(query2, con=engine, params={'app_id': id_key})
	if len(df2)==0:
		status_2 = 0
	else:
		status_2 = 1

	if status_1 ==-1 and status_2 == 0:
		passed = 1
		notes='Δεν έχει mmz και δεν έχει δηλωθεί 0407'
	if status_1 == 0 and status_2 == 0:
		passed = 1
		notes='mmz<3 και δεν έχει δηλωθεί 0407'
	if status_1 == 1 and status_2 == 1:
		passed = 1
		notes='mmz>=3 και έχει δηλωθεί 0407'
	if status_1 == 1 and status_2 == 0:
		passed = 0
		notes='mmz>=3 και δεν έχει δηλωθεί 0407'
	if status_1 == 0 and status_2 == 1:
		passed = 0
		notes='mmz<3 και έχει δηλωθεί 0407'

	update_application_checks(id_key, tag, passed, notes)
	