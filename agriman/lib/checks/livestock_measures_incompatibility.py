from sqlalchemy import text
import pandas as pd
import itertools
from agriman.database import get_engine, update_application_checks, sql_safe

def check_livestock_measures_incompatibility(id_key):
	tag = 'livestock_measures_incompatibility'
	engine = get_engine()
	
	fid=open('log_check_livestock_measures_incompatibility.txt','w')
	query1 = text("""
    SELECT
        applications.afm,
        applications.year,
        application_measures.code AS code
    FROM applications
    JOIN application_measures ON applications.id = application_measures.application_id
    WHERE applications.id = :app_id AND application_measures.code IN ('Π3.70.2.1.2','Π3.70.1.5')
    ORDER BY application_measures.code
  """)
	
	df1 = pd.read_sql(query1, con=engine, params={'app_id': id_key})
	if len(df1) == 0:
		status_1 = -1
	else:
		df1['code']=df1['code'].str[3:]
		meas_list = df1["code"].tolist()
		if len(meas_list) == 1:
			status_1 = 0
		else:
			status_1 = 1
			comb_list = [list(c) for c in itertools.combinations(meas_list, 2)]
			list_incomp=[]
			for comb in comb_list:
				# query3 = f"""
				# SELECT
				# 	corresponds.source
				# FROM corresponds 
				# WHERE corresponds.scope = 'measure' AND corresponds.target = '{comb[0]}' 
				# """
				# df3=pd.read_sql(query3, con=engine)
				# if df3.empty:
				# 	print(comb[0])
				# val3= df3.loc[0, "source"]
				# query4 = f"""
				# SELECT
				# 	corresponds.source
				# FROM corresponds 
				# WHERE corresponds.scope = 'measure' AND corresponds.target = '{comb[1]}' 
				# """
				# df4=pd.read_sql(query4, con=engine)
				# if df4.empty:
				# 	print(comb[1])
				# val4= df4.loc[0, 'source']
				query5 = text("""
					SELECT
						correlations.value
					FROM correlations 
					WHERE correlations.scope = 'table4'  AND correlations.source = :comb0 AND correlations.target = :comb1
				""")
				df5=pd.read_sql(query5, con=engine, params={'comb0': sql_safe(comb[0]), 'comb1': sql_safe(comb[1])})
				if len(df5) == 0:
					status_2 = 0
					fid.write(f'Not exist {comb[0]} or {comb[1]} in correlations\n')
				else:
					value= df5.loc[0, 'value']
					if value == 0:
						status_2 = 0
						list_incomp.append(comb)
					else:
						status_2 = 1
	
	if status_1 == -1:
		passed = 1
		notes='Δεν έχουν δηλωθεί Μέτρα'
	elif status_1 == 0:
		passed = 1
		notes='Έχει δηλωθεί ένα Μέτρο'
	else:
		if status_2 == 0:
			passed = 1
			notes='Δεν υπάρχουν ασυμβατότητες μεταξύ των Μέτρων'
		else:
			passed = 0
			s = "\n".join([" , ".join(sublist) for sublist in list_incomp])
			notes='Υπάρχουν ασυμβατότητες μεταξύ των Μέτρων: ' + s

	update_application_checks(id_key, tag, passed, notes)
	