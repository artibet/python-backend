from sqlalchemy import text
import pandas as pd
from agriman.database import get_engine, update_application_checks

def check_application_atak(id_key):
	tag = 'application_atak'
	engine = get_engine()
	
	query1 = text("""
    SELECT
      applications.afm,
      applications.year,
      parcels.code AS aa,
      parcels.area AS parcel_area,
      contracts.atak,
      contracts.area AS atak_area
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
		df1['a_area']=df1['atak_area'].where(df1['atak'].notna(),0)
		res = (
			df1.groupby('aa', as_index=False)
			.agg(
				parcel_area=('parcel_area', 'first'),
				total_atak_area=('a_area', 'sum'),
				count_atak_true=('atak','count'),
				count_atak_all=('aa','size')
			)
		) 
		res['parcel_area']=pd.to_numeric(res['parcel_area'], errors='coerce').round(2)
		res['total_atak_area']=pd.to_numeric(res['total_atak_area'], errors='coerce').round(2)
		res_list=res.values.tolist()
		msmall=[]
		status_1 = 1
		for row in res_list:
			if not (row[1]<=row[2] and row[3]==row[4]):
				status_1 = 0
				msm=f'Αγροτεμαχιο {row[0]}: Έκταση ΑΤΑΚ/αγροτεμαχιο = {row[2]}/{row[1]}, πλήθος ΑΤΑΚ/Όλα = {row[3]}/{row[4]} '
				msmall.append(msm)

	if status_1 == -1:
		passed = 0
		notes='Δεν έχουν δηλωθεί ATAK'
	elif status_1 == 1:
		passed = 1
		notes='Έχουν δηλωθεί όλα τα ATAK'
	else:
		passed = 0
		notes = "\n".join(msmall)
    
	update_application_checks(id_key, tag, passed, notes)
	


