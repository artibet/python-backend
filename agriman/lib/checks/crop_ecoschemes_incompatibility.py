from sqlalchemy import text
import pandas as pd
import itertools
from agriman.database import get_engine, update_application_checks

def check_crop_ecoschemes_incompatibility(id_key):
	tag = 'crop_echoschemes_incompatibility'
	engine = get_engine()
	
	fid=open('log_check_crop_ecoschemes_incompatibility.txt','w')
	query1 = text("""
    SELECT
      applications.afm,
      applications.year,
      parcels.code AS aa,
      ecoschemes.code AS code
    FROM parcels
    JOIN applications ON applications.id = parcels.application_id
    JOIN ecoschemes ON ecoschemes.parcel_id = parcels.id
    WHERE applications.id = :app_id AND parcels.is_seed_cultivation = 0 AND ecoschemes.code NOT IN('ECO-06.01')
    ORDER BY parcels.code
	""")
	df1=pd.read_sql(query1, con=engine, params={'app_id': id_key})
	if len(df1) == 0:
		status_1 = -1
	else:
		status_1 = 0
		aa_dict = df1.groupby("aa")["code"].apply(list).to_dict()
		list_reports=[]
		for key in aa_dict.keys():
			if len(aa_dict[key])<=1:
				continue
			eco_list = aa_dict[key]
			comb_list = [list(c) for c in itertools.combinations(eco_list, 2)]
			for comb in comb_list:
				query3 = text("""
					SELECT
						corresponds.source
					FROM corresponds 
					WHERE corresponds.scope = 'ecoscheme' AND corresponds.target = :comb 
				""")
				df3=pd.read_sql(query3, con=engine, params={'comb': comb[0]})
				if df3.empty:
					fid.write(f'Not exist {comb[0]} in corresponds\n')
					continue
				val3= df3.loc[0, "source"]
				query4 = text("""
					SELECT
						corresponds.source
					FROM corresponds 
					WHERE corresponds.scope = 'ecoscheme' AND corresponds.target = :comb 
				""")
				df4=pd.read_sql(query4, con=engine, params={'comb': comb[1]})
				if df4.empty:
					fid.write(f'Not exist {comb[1]} in corresponds\n')
					continue
				val4= df4.loc[0, 'source']
				query5 = text("""
					SELECT
						correlations.value
					FROM correlations 
					WHERE correlations.scope = 'table1'  AND correlations.source = :val3 AND correlations.target =  :val4
				""")
				df5=pd.read_sql(query5, con=engine, params={'val3': val3, 'val4': val4})
				if len(df5) == 0:
					fid.write(f'Not exist {val3} or {val4} in correlations \n')
				else:
					value= df5.loc[0, 'value']
					if value == 0:
						list_reports.append(f'Α/Α {key} : υπάρχει ασυμβατότητα μεταξύ {val3} και {val4}')

	if status_1 == -1:
		passed = 1
		notes='Δεν έχουν δηλωθεί Οικοσχήματα'
	else:
		if len(list_reports)==0:
			passed = 1
			notes='Δεν υπάρχουν ασυμβατότητες μεταξύ των Οικοσχημάτων'
		else:
			passed = 0
			notes="\n".join(list_reports)

	update_application_checks(id_key, tag, passed, notes)
	