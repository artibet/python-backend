from sqlalchemy import text
import pandas as pd
import itertools
from agriman.database import get_engine, update_application_checks


def check_livestock_echoschemes_incompatibility(id_key):
	tag = 'livestock_echoschemes_incompatibility'
	engine = get_engine()
	
	fid=open('log_check_livestock_echoschemes_incompatibility.txt','w')
	query1 = text("""
    SELECT
      applications.afm,
      applications.year,
      application_ecoschemes.code AS code
    FROM applications
    JOIN application_ecoschemes ON application_ecoschemes.application_id = applications.id
    WHERE applications.id = :app_id AND application_ecoschemes.code IN ('Π1-31.9-Β','Π1-31.5-Β','Π1-31.7-Δ') 
    ORDER BY application_ecoschemes.code
	""")
	df1=pd.read_sql(query1, con=engine, params={'app_id': id_key})
	if len(df1) == 0:
		status_1 = -1
	else:
		df1['code'] = df1['code'].str[3:]
		df1['code'] = df1['code'].replace("31.9-Β", "31.9")
		eco_list = df1["code"].tolist()
		if len(eco_list) == 1:
			status_1 = 0
		else:
			status_1 = 1
			comb_list = [list(c) for c in itertools.combinations(eco_list, 2)]
			list_incomp=[]
			for comb in comb_list:
				# query3 = f"""
				# SELECT
				# 	corresponds.source
				# FROM corresponds 
				# WHERE corresponds.scope = 'ecoscheme' AND corresponds.target = '{comb[0]}' 
				# """
				# df3=pd.read_sql(query3, con=engine)
				# if df3.empty:
				# 	print(comb[0])
				# val3= df3.loc[0, "source"]
				# query4 = f"""
				# SELECT
				# 	corresponds.source
				# FROM corresponds 
				# WHERE corresponds.scope = 'ecoscheme' AND corresponds.target = '{comb[1]}' 
				# """
				# df4=pd.read_sql(query4, con=engine)
				# if df4.empty:
				# 	print(comb[1])
				# val4= df4.loc[0, 'source']
				query5 = text("""
					SELECT
						correlations.value
					FROM correlations 
					WHERE correlations.scope = 'table1'  AND correlations.source = :comb0 AND correlations.target = :comb1
				""")
				df5=pd.read_sql(query5, con=engine, params={'comb0': comb[0], 'comb1': comb[1]})
				if len(df5) == 0:
					status_2 = 0
					fid.write(f'Not exist {comb[0]} or {comb[1]} in correlations\n')
				else:
					value= df5.loc[0, 'value']
					if value == 0:
						status_2 = 1
						list_incomp.append(comb)
					else:
						status_2 = 0

	if status_1 == -1:
		passed = 1
		notes='Δεν έχουν δηλωθεί Οικοσχήματα'
	elif status_1 == 0:
		passed = 1
		notes='Έχει δηλωθεί ένα Οικοσχήμα'
	else:
		if status_2 == 0:
			passed = 1
			notes='Δεν υπάρχουν ασυμβατότητες μεταξύ των Οικοσχημάτων'
		else:
			passed = 0
			s = "\n".join([" , ".join(sublist) for sublist in list_incomp])
			notes='Υπάρχουν ασυμβατότητες μεταξύ των Οικοσχημάτων: \n' + s

	update_application_checks(id_key, tag, passed, notes)