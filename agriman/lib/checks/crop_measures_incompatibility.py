from sqlalchemy import text
import pandas as pd
import itertools
from agriman.database import get_engine, update_application_checks, sql_safe

def check_crop_measures_incompatibility(id_key):
	tag = 'crop_measures_incompatibility'
	engine = get_engine()
	
	fid=open('log_check_crop_measures_incompatibility.txt','w')
	query1 = text("""
    SELECT
        applications.afm,
        applications.year,
		parcels.code AS aa,
        measures.code AS code
    FROM measures
    JOIN parcels ON parcels.id = measures.parcel_id
    JOIN applications ON applications.id = parcels.application_id
    WHERE applications.id = :app_id AND measures.code LIKE :code_pattern
    ORDER BY measures.code
  """)
	df1 = pd.read_sql(query1, con=engine, params={'app_id': id_key, 'code_pattern': 'Π3.70%'})
	if len(df1) == 0:
		status_1 = -1
	else:
		status_1 = 0
		aa_dict = df1.groupby("aa")["code"].apply(list).to_dict()
		list_reports=[]
		for key in aa_dict.keys():
			if len(aa_dict[key])<=1:
				continue
			meas_list = aa_dict[key]
			comb_list = [list(c) for c in itertools.combinations(meas_list, 2)]
			for comb in comb_list:
				query3 = text("""
					SELECT
						corresponds.source
					FROM corresponds 
					WHERE corresponds.scope = 'measure' AND corresponds.target = :comb 
				""")
				df3=pd.read_sql(query3, con=engine, params={'comb': sql_safe(comb[0])})
				if df3.empty:
					fid.write(f'Not exist {comb[0]} in corresponds\n')
					continue
				val3= df3.loc[0, "source"]
				query4 = text("""
					SELECT
						corresponds.source
					FROM corresponds 
					WHERE corresponds.scope = 'measure' AND corresponds.target = :comb 
				""")
				df4=pd.read_sql(query4, con=engine, params={'comb': sql_safe(comb[1])})
				if df4.empty:
					fid.write(f'Not exist {comb[1]} in corresponds\n')
					continue
				val4= df4.loc[0, 'source']
				query5 = text("""
					SELECT
						correlations.value
					FROM correlations 
					WHERE correlations.scope = 'table4'  AND correlations.source = :val3 AND correlations.target = :val4
				""")
				df5=pd.read_sql(query5, con=engine, params={'val3': val3, 'val4': val4})
				if len(df5) == 0:
					fid.write(f'Not exist {val3} or {val4} in correlations\n')
				else:
					value= df5.loc[0, 'value']
					if value == 0:
						list_reports.append(f'Α/Α {key} : υπάρχει ασυμβατότητα μεταξύ {val3} και {val4}')

	if status_1 == -1:
		passed = 1
		notes='Δεν έχουν δηλωθεί Μέτρα'
	else:
		if len(list_reports)==0:
			passed = 1
			notes='Δεν υπάρχουν ασυμβατότητες μεταξύ των Μέτρων'
		else:
			passed = 0
			notes="\n".join(list_reports)

	update_application_checks(id_key, tag, passed, notes)
	