from sqlalchemy import text
import pandas as pd
from agriman.database import get_engine, update_application_checks

def check_corn_irrigation(id_key):
	tag = 'corn_irrigation'
	engine = get_engine()
	
	#### SQL ερώτημα
	query1 = text("""
	SELECT
	    applications.afm,
		cultivation_forms.code,
		parcel_cultivations.is_irrigated,
		parcels.code AS aa
	FROM applications 
	JOIN parcels ON parcels.application_id = applications.id
	JOIN parcel_cultivations ON parcel_cultivations.parcel_id = parcels.id
	JOIN cultivation_forms ON cultivation_forms.id = parcel_cultivations.cultivation_form_id
	WHERE applications.id = :app_id  AND cultivation_forms.code IN ('3.1','3.2')
    ORDER BY cultivation_forms.code
	""")
	df1=pd.read_sql(query1, con=engine, params={'app_id': id_key})

	if len(df1)==0:
		status_1 = -1
	elif len(df1)>0:
		df2=df1[df1['is_irrigated'] == 0]
		if len(df2)==0:
			status_1 = 1
		else:
			status_1 = 0

	if status_1 == -1:
		passed = 1
		notes='Δεν έχουν δηλωθει 3.1 και 3.2'	
	if status_1 == 1:
		passed = 1
		notes='Όλα τα 3.1 και 3.2 είναι αρδευόμενα'
	if status_1 == 0:
		passed = 0
		notes='Δεν έχουν δηλωθεί ως αρδευόμενα τα αγροτεμάχια με ΑΑ:'
		rows=len(df2)
		for row in range(rows):
			val=df2.iloc[row]['aa']
			notes = notes +' '+ val

	update_application_checks(id_key, tag, passed, notes)
