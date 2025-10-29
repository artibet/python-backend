from sqlalchemy import text
import pandas as pd
import numpy as np
from agriman.database import get_engine, update_application_checks

def check_application_sublease(id_key):
	tag = 'application_sublease'
	engine = get_engine()
	
	query1 = text("""
    SELECT
	    applications.afm,
    	applications.year,
      	parcels.code AS aa,
      	contracts.atak,
      	contracts.sublease,
		contracts.afm AS contracts_afm	
    FROM applications
    JOIN parcels ON parcels.application_id = applications.id
    JOIN contracts ON contracts.parcel_id = parcels.id
    WHERE applications.id = :app_id 
    ORDER BY parcels.code
	""")
	df1 = pd.read_sql(query1, con=engine, params={'app_id': id_key})
	if len(df1) == 0:
		status_1 = -1
	else:
		df = df1.copy()
		df["afm"] = df["afm"].astype(str).str.strip()
		df["contracts_afm"] = df["contracts_afm"].astype(str).str.strip()
		df["sublease"] = pd.to_numeric(df["sublease"], errors="coerce").fillna(0)

		# Υπολογισμός status ανα aa
		status_df = df.groupby("aa").apply(
			lambda g: 0 if g["contracts_afm"].isin(g["afm"]).any()
			else 1 if (g["sublease"] == 1).all()
			else 2,
			include_groups=False
		).reset_index(name="status")

		res_list=status_df.values.tolist()
		msmall=[]
		status_1 = 1
		for row in res_list:
			if row[1]==2:
				status_1 = 0
				msm=f'Αγροτεμαχιο {row[0]}: Δεν δηλώθηκε το Sublease ΝΑΙ '
				msmall.append(msm)

	if status_1 == -1:
		passed = 1
		notes='Δεν υπάρχουν contracts'
	elif status_1 == 1:
		passed = 1
		notes='AFM υπάρχει στα contracts ή AFM δεν υπάρχει και όλα sublease=1'
	else:
		passed = 0
		notes = "\n".join(msmall)
    
	update_application_checks(id_key, tag, passed, notes)