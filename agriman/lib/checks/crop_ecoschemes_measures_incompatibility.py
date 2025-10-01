from sqlalchemy import text
import pandas as pd
import itertools
from agriman.database import get_engine, update_application_checks, sql_safe

def check_crop_ecoschemes_measures_incompatibility(id_key):
	tag = 'crop_ecoschemes_measures_incompatibility'
	engine = get_engine()
	
	fid=open('log_check_crop_ecoschemes_measures_incompatibility.txt','w')
	query1 = text("""
    SELECT
      applications.afm,
      applications.year,
      parcels.code AS aa,
      ecoschemes.code AS code
    FROM parcels
    JOIN applications ON applications.id = parcels.application_id
    JOIN ecoschemes ON ecoschemes.parcel_id = parcels.id
    WHERE applications.id = :app_id AND parcels.is_seed_cultivation = 0
    ORDER BY ecoschemes.code
	""")
	df1 = pd.read_sql(query1, con=engine, params={'app_id': id_key})
	query2 = text("""
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
	df2=pd.read_sql(query2, con=engine, params={'app_id': id_key, 'code_pattern': 'Π3.70%'})

	if len(df1) == 0 or len(df2) == 0:
		status_1 = -1
	else:
		aa_dict1 = df1.groupby("aa")["code"].apply(list).to_dict()
		aa_dict2 = df2.groupby("aa")["code"].apply(list).to_dict()
		merged_dict={k:[aa_dict1[k],aa_dict2[k]] for k in aa_dict1.keys() & aa_dict2.keys()}
		if len(merged_dict)==0:
			status_1 = 0
		else:
			status_1 = 1
			list_reports=[]
			for key in merged_dict.keys():
				eco_list = merged_dict[key][0]
				meas_list = merged_dict[key][1]
				combinations = [list(pair) for pair in itertools.product(eco_list, meas_list)]
				for comb in combinations:
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
						WHERE corresponds.scope = 'measure' AND corresponds.target = :comb 
					""")
					df4=pd.read_sql(query4, con=engine, params={'comb': sql_safe(comb[1])})
					if df4.empty:
						fid.write(f'Not exist {comb[1]} corresponds\n')
						continue
					val4= df4.loc[0, 'source']
					query5 = text("""
						SELECT
							correlations.value
						FROM correlations 
						WHERE correlations.scope = 'table3'  AND correlations.source = :val3 AND correlations.target = :val4
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
		notes='Δεν έχουν δηλωθεί είτε Οικοσχήματα είτε Μέτρα '
	elif status_1 == 0:
		passed = 1
		notes='Δεν υπάρχουν συνδυασμοί ανα Α/Α μεταξύ Οικοσχημάτων και Μέτρων'
	else:
		if len(list_reports)==0:
			passed = 1
			notes='Δεν υπάρχουν ασυμβατότητες μεταξύ Οικοσχημάτων και Μέτρων'
		else:
			passed = 0
			notes="\n".join(list_reports)

	update_application_checks(id_key, tag, passed, notes)
