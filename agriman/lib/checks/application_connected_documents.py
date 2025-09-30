from sqlalchemy import text
import pandas as pd
import json
from agriman.database import get_engine, update_application_checks

def check_application_connected_documents(id_key):
	tag = 'application_connected_documents'
	engine = get_engine()
	
	query1 = text("""
    SELECT
      applications.afm,
      applications.year,
      parcels.code AS aa,
      support_schemes.code AS code
    FROM applications
    JOIN parcels ON parcels.application_id = applications.id
    JOIN contracts ON contracts.parcel_id = parcels.id
    JOIN parcel_cultivations ON parcels.id = parcel_cultivations.parcel_id
    JOIN support_varieties ON support_varieties.variety_id = parcel_cultivations.variety_id
    JOIN supports ON supports.id = support_varieties.support_id
    LEFT OUTER JOIN support_schemes ON support_schemes.parcel_cultivation_id = parcel_cultivations.id
    WHERE applications.id = :app_id AND supports.period_id = applications.period_id 
    AND support_schemes.code IN ('0501','0114')
    ORDER BY parcels.code
	""")
	df1 = pd.read_sql(query1, con=engine, params={'app_id': id_key})
	if len(df1) == 0:
		status_1 = -1
	else:
		status_1 = 0
		query2 = text("""
			SELECT
				applications.afm,
				applications.year,
				applications.json
			FROM applications
			WHERE applications.id = :app_key
		""")
		df2 = pd.read_sql(query2, con=engine, params={'app_key': id_key})
		df2["parsed"] = df2["json"].apply(json.loads)
		df2["doc_codes"] = df2["parsed"].apply(lambda x: [d["document_type_code"] for d in x.get("application_document_list", [])])
		scodes = df1["code"].unique().tolist()
		type_codes=df2.loc[0,'doc_codes']
		msm=[]
		status_2 = 0
		for scode in scodes:
			if scode == '0114':
				if '178' not in type_codes:
					status_2 = 1
					msm.append('Δεν έχει δηλωθεί το 178')
				if '179' not in type_codes:
					status_2 = 1
					msm.append('Δεν έχει δηλωθεί το 179')
			if scode == '0501':
				if '7' not in type_codes: 
					status_2 = 1
					msm.append('Δεν έχει δηλωθεί το 7')
				if '64' not in type_codes: 
					status_2 = 1
					msm.append('Δεν έχει δηλωθεί το 64')

	if status_1 == -1:
		passed = 1
		notes='Δεν έχουν δηλωθεί συνδεδεμένες 0114 ή 0501'
	else:
		if status_2 == 0:
			passed = 1
			notes = 'Έχουν δηλωθεί τα δικαολογητικά για συνδεδεμένες 0114 ή 0501'
		elif status_2 == 1:
			passed = 0
			notes = "\n".join(msm)
		
	update_application_checks(id_key, tag, passed, notes)
