from sqlalchemy import text
import pandas as pd
import itertools
from agriman.database import get_engine, update_application_checks, sql_safe

def check_livestock_ecoschemes_measures_incompatibility(id_key):
	tag = 'livestock_ecoschemes_measures_incompatibility'
	engine = get_engine()
	
	fid=open('log_check_livestock_ecoschemes_measures_incompatibility.txt','w')
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
	df1 = pd.read_sql(query1, con=engine, params={'app_id': id_key})
	query2 = text("""
    SELECT
        applications.afm,
        applications.year,
        application_measures.code AS code
    FROM applications
    JOIN application_measures ON applications.id = application_measures.application_id
    WHERE applications.id = :app_id AND application_measures.code IN ('Π3.70.2.1.2','Π3.70.1.5')
    ORDER BY application_measures.code
    """)
	df2=pd.read_sql(query2, con=engine, params={'app_id': id_key})

	if len(df1) == 0 or len(df2) == 0:
		status_1 = -1
	else:
		df1['code']=df1['code'].str[3:]
		df1['code'] = df1['code'].replace("31.9-Β", "31.9")
		eco_list = df1["code"].tolist()
		df2['code']=df1['code'].str[3:]
		meas_list = df2["code"].tolist()
		combinations = [list(pair) for pair in itertools.product(eco_list, meas_list)]
		status_1 = 1
		list_incomp=[]
		for comb in combinations:
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
				WHERE correlations.scope = 'table3'  AND correlations.source = :comb0 AND correlations.target = :comb1
			""")
			df5=pd.read_sql(query5, con=engine, params={'comb0': sql_safe(comb[0]), 'comb1': sql_safe(comb[1])})
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
		notes='Δεν έχουν δηλωθεί είτε Οικοσχήματα είτε Μέτρα '
	else:
		if status_2 == 0:
			passed = 1
			notes='Δεν υπάρχουν ασυμβατότητες μεταξύ Οικοσχημάτων και Μέτρων'
		else:
			passed = 0
			s = "\n".join([" , ".join(sublist) for sublist in list_incomp])
			notes='Υπάρχουν ασυμβατότητες μεταξύ Οικοσχημάτων και Μέτρων: ' + s

	update_application_checks(id_key, tag, passed, notes)
	