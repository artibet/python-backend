from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
from datetime import date

import itertools

from openpyxl import load_workbook


## SQLAlchemy engine

host="83.212.59.99"      # IP της βάσης στο δίκτυο
user="root"
password="Pa$$w0rdBL11"
database="agriman"

engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

# # Φόρτωση των αρχείων Excel
# ##support_schemes_df = pd.read_excel("Support_schemes.xlsx")
# dfA = pd.read_excel("../Excel_Inputs/applications_2025.xlsx", dtype={'afm': str}, engine='openpyxl')
# dfL = pd.read_excel("../Excel_Inputs/livestock_2025.xlsx", dtype={'afm': str}, engine='openpyxl')
# dfC = pd.read_excel("../Excel_Inputs/cultivations_2025.xlsx", dtype={'afm': str}, engine='openpyxl')




def check_esap(id_key):
	query1 = f"""
	SELECT
	    applications.esap
	FROM applications 
	WHERE applications.id = '{id_key}' 
	"""
	df1=pd.read_sql(query1, con=engine)
	if df1.empty:
		status_1 = 0
	else:
		val = df1.loc[0, 'esap']
		if pd.isna(val) or val == "" or val is None:
			status_1 = 0
		else:
			status_1 = 1

	query2 = f"""
	SELECT
	    applications.afm,
	    ecoschemes.code
	FROM applications 
	JOIN parcels ON parcels.application_id = applications.id
	JOIN ecoschemes ON ecoschemes.parcel_id=parcels.id
	WHERE applications.id = '{id_key}' AND ecoschemes.code LIKE 'ECO-06.%'
    ORDER BY ecoschemes.code
	"""
	df2=pd.read_sql(query2, con=engine)
	if len(df2) == 0:
		status_2 = 0
	else:
		status_2 = 1

	if status_1 == 0 and status_2 == 0:
		passed = 1
		notes='Δεν έχει δηλωθεί Ψηφιακή Εφαρμογή και δεν έχει δηλωθεί Οικοσχήμα της μορφής ECO-06.*'
	if status_1 == 1 and status_2 == 1:
		passed = 1
		notes='Έχει δηλωθεί Ψηφιακή Εφαρμογή και έχει δηλωθεί Οικοσχήμα της μορφής ECO-06.*'
	if status_1 == 1 and status_2 == 0:
		passed = 0
		notes='Έχει δηλωθεί Ψηφιακή Εφαρμογή και δεν έχει δηλωθεί Οικοσχήμα της μορφής ECO-06.*'
	if status_1 == 0 and status_2 == 1:
		passed = 0
		notes='Δεν έχει δηλωθεί Ψηφιακή Εφαρμογή και έχει δηλωθεί Οικοσχήμα της μορφής ECO-06.*'

	check_id = 1

	with engine.begin() as conn:
		conn.execute(text(f"""
			UPDATE application_checks
			SET checked_at = UTC_TIMESTAMP(),
				passed = {passed},
				notes = '{notes}'
			WHERE application_id = {id_key} AND check_id = {check_id}
		"""))

	return()


def check_pasture_mmz(id_key):
	query1 = f"""
	SELECT
	    applications.mmz_total
	FROM applications 
	WHERE applications.id = '{id_key}' 
	"""
	df1=pd.read_sql(query1, con=engine)
	if len(df1)==0:
		status_1 = -1
	else:
		val = df1.loc[0, 'mmz_total']
		if val < 3:
			status_1 = 0
		else:
			status_1 = 1

	query2 = f"""
	SELECT
	    applications.afm,
	    application_support_schemes.code
	FROM applications 
	JOIN application_support_schemes ON application_support_schemes.application_id = applications.id
	WHERE applications.id = '{id_key}'  AND application_support_schemes.code = '0407'
    ORDER BY application_support_schemes.code
	"""
	df2=pd.read_sql(query2, con=engine)
	if len(df2)==0:
		status_2 = 0
	else:
		status_2 = 1

	if status_1 ==-1 and status_2 == 0:
		passed = 1
		notes='Δεν έχει mmz και δεν έχει δηλωθεί 0407'
	if status_1 == 0 and status_2 == 0:
		passed = 1
		notes='mmz<3 και δεν έχει δηλωθεί 0407'
	if status_1 == 1 and status_2 == 1:
		passed = 1
		notes='mmz>=3 και έχει δηλωθεί 0407'
	if status_1 == 1 and status_2 == 0:
		passed = 0
		notes='mmz>=3 και δεν έχει δηλωθεί 0407'
	if status_1 == 0 and status_2 == 1:
		passed = 0
		notes='mmz<3 και έχει δηλωθεί 0407'

	check_id = 2

	with engine.begin() as conn:
		conn.execute(text(f"""
			UPDATE application_checks
			SET checked_at = UTC_TIMESTAMP(),
				passed = {passed},
				notes = '{notes}'
			WHERE application_id = {id_key} AND check_id = {check_id}
		"""))

	return()


def check_corn_irrigation(id_key):
	#### SQL ερώτημα
	query1 = f"""
	SELECT
	    applications.afm,
		cultivation_forms.code,
		parcel_cultivations.is_irrigated,
		parcels.code AS aa
	FROM applications 
	JOIN parcels ON parcels.application_id = applications.id
	JOIN parcel_cultivations ON parcel_cultivations.parcel_id = parcels.id
	JOIN cultivation_forms ON cultivation_forms.id = parcel_cultivations.cultivation_form_id
	WHERE applications.id = '{id_key}'  AND cultivation_forms.code IN ('3.1','3.2')
    ORDER BY cultivation_forms.code
	"""
	df1=pd.read_sql(query1, con=engine)

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

	check_id = 3

	with engine.begin() as conn:
		conn.execute(text(f"""
			UPDATE application_checks
			SET checked_at = UTC_TIMESTAMP(),
				passed = {passed},
				notes = '{notes}'
			WHERE application_id = {id_key} AND check_id = {check_id}
		"""))

	return()


def check_national_reserve(id_key):
	query1 = f"""
	SELECT
	    applications.first_submission
	FROM applications 
	WHERE applications.id = '{id_key}'  AND applications.first_submission = '2025'
	"""
	df1=pd.read_sql(query1, con=engine)
	if len(df1)==0:
		status_1 = 0
	else:
		status_1 = 1

	query2 = f"""
	SELECT
	    applications.afm,
	    application_support_schemes.code
	FROM applications 
	JOIN application_support_schemes ON application_support_schemes.application_id = applications.id
	WHERE applications.id = '{id_key}'  AND application_support_schemes.code = '0402'
    ORDER BY application_support_schemes.code
	"""
	df2=pd.read_sql(query2, con=engine)
	if len(df2)==0:
		status_2 = 0
	else:
		status_2 = 1

	if status_1 == 0 and status_2 == 0:
		passed = 1
		notes='Δεν έχει πρώτη δήλωση το 2025 και δεν έχει δηλωθεί 0402'
	if status_1 == 1 and status_2 == 1:
		passed = 1
		notes='Έχει πρώτη δήλωση το 2025 και έχει δηλωθεί 0402'
	if status_1 == 1 and status_2 == 0:
		passed = 0
		notes='Έχει πρώτη δήλωση το 2025 και δεν έχει δηλωθεί 0402'
	if status_1 == 0 and status_2 == 1:
		passed = 0
		notes='Δεν έχει πρώτη δήλωση το 2025 και έχει δηλωθεί 0402'

	check_id = 4

	with engine.begin() as conn:
		conn.execute(text(f"""
			UPDATE application_checks
			SET checked_at = UTC_TIMESTAMP(),
				passed = {passed},
				notes = '{notes}'
			WHERE application_id = {id_key} AND check_id = {check_id}
		"""))

	return()


def check_young_farmers(id_key):
	query1 = f"""
	SELECT
		applications.first_submission,
		applications.birth_date
	FROM applications 
	WHERE applications.id = '{id_key}' and (first_submission IS NOT null AND birth_date IS NOT null) 
	"""
	df1=pd.read_sql(query1, con=engine)

	if len(df1)==0:
		status_1 = -1
	else:
		df1['birth_date']=pd.to_datetime(df1['birth_date'])
		val=df1.loc[0,'birth_date']
		age=date.today().year - val.year
		if age <= 44:
			status_1 = 1
		else:
			status_1 = 0
	
		val2=df1.loc[0,'first_submission']
		if 2025-val2<=4: 
			status_2 = 1
		else:
			status_2 = 0
	
	query2 = f"""
	SELECT
	    applications.afm,
	    application_support_schemes.code
	FROM applications 
	JOIN application_support_schemes ON application_support_schemes.application_id = applications.id
	WHERE applications.id = '{id_key}'  AND application_support_schemes.code = '0201'
    ORDER BY application_support_schemes.code
	"""
	df2=pd.read_sql(query2, con=engine)
	if len(df2)==0:
		status_3 = 0
	else:
		status_3 = 1

	passed=None
	notes=''
	if status_1 == -1:
		passed = 1
		notes='Δεν έχει δηλώσει δεδομένα σχετικά με ηλικία και πρώτη υποβολή'
	elif status_1 == 0:
		passed = 0
		notes='Ηλικία > 44'
	elif status_1 == 1:
		if status_2 == 1:
			if status_3 == 1:
				passed = 1
				notes='Γεωργός Μικρής Ηλικίας με 0201'
			else:
				passed = 0
				notes='Γεωργός Μικρής Ηλικίας χωρίς 0201'
		else:
			if status_3 == 1:
				passed = 0
				notes='Γεωργός Μικρής Ηλικίας με 0201 με πρώτη δήλωση'+ str(val2)
			else:
				passed = 0
				notes='Γεωργός Μικρής Ηλικίας χωρίς 0201 πρώτη δήλωση'+ str(val2)
	else:
		print("OK")

	check_id = 5

	with engine.begin() as conn:
		conn.execute(text(f"""
			UPDATE application_checks
			SET checked_at = UTC_TIMESTAMP(),
				passed = {passed},
				notes = '{notes}'
			WHERE application_id = {id_key} AND check_id = {check_id}
		"""))

	return()

def check_application_ecoschemes(id_key):
	query1 = f"""
	SELECT
		applications.afm,
		applications.year,
		ecoschemes.code AS code
	FROM parcels
	JOIN applications ON applications.id = parcels.application_id
	JOIN ecoschemes ON ecoschemes.parcel_id = parcels.id
	WHERE applications.id = '{id_key}' AND parcels.is_seed_cultivation = 0
	GROUP BY applications.afm, ecoschemes.code
	ORDER BY ecoschemes.code
	"""
	df1=pd.read_sql(query1, con=engine)

	if len(df1) == 0:
		status_1 = 0
	else:
		status_1 = 1
		query2 = f"""
		SELECT
			applications.afm,
			application_ecoschemes.code
		FROM applications 
		JOIN application_ecoschemes ON application_ecoschemes.application_id = applications.id
		WHERE applications.id = '{id_key}' 
		ORDER BY application_ecoschemes.code
		"""
		df2=pd.read_sql(query2, con=engine)
		if len(df2) == 0:
			status_2 = 0
		else:
			status_2 = 1
			# query3 = f"""
			# SELECT
			# 	corresponds.target,
			# FROM corresponds 
			# WHERE corresponds.scope = ecoscheme AND corresponds.source = '{eco}' 
			# ORDER BY application_ecoschemes.code
			# """
			# df3=pd.read_sql(query3, con=engine)


	passed=None
	notes=''
	if status_1 == 0:
		passed = 1
		notes='Δεν έχουν δηλωθεί Οικοσχήματα'
	else:
		codes_str = ", ".join(df1["code"].tolist())
		if status_2 == 0:
			passed = 0
			notes='Έχουν δηλωθεί τα Οικοσχήματα: '+ codes_str +' δεν υπάρχουν στην δήλωση'
		else:
			passed = 0
			codes_str2 = ", ".join(df2["code"].tolist())
			notes='Έχουν δηλωθεί τα Οικοσχήματα: '+ codes_str +' και υπάρχουν στην δήλωση: '+ codes_str2

	check_id = 6

	with engine.begin() as conn:
		conn.execute(text(f"""
			UPDATE application_checks
			SET checked_at = UTC_TIMESTAMP(),
				passed = {passed},
				notes = '{notes}'
			WHERE application_id = {id_key} AND check_id = {check_id}
		"""))

	return()


def check_crop_echoschemes_incompatibility(id_key):
	query1 = f"""
	SELECT
		applications.afm,
		applications.year,
		ecoschemes.code AS code
	FROM parcels
	JOIN applications ON applications.id = parcels.application_id
	JOIN ecoschemes ON ecoschemes.parcel_id = parcels.id
	WHERE applications.id = '{id_key}' AND parcels.is_seed_cultivation = 0
	GROUP BY applications.afm, ecoschemes.code
	ORDER BY ecoschemes.code
	"""
	df1=pd.read_sql(query1, con=engine)

	if len(df1) == 0:
		status_1 = -1
	else:
		eco_list = df1["code"].tolist()
		if len(eco_list) == 1:
			status_1 = 0
		else:
			status_1 = 1
			comb_list = [list(c) for c in itertools.combinations(eco_list, 2)]
			list_incomp=[]
			for comb in comb_list:
				query3 = f"""
				SELECT
					corresponds.source
				FROM corresponds 
				WHERE corresponds.scope = 'ecoscheme' AND corresponds.target = '{comb[0]}' 
				"""
				df3=pd.read_sql(query3, con=engine)
				if df3.empty:
					print(comb[0])
				val3= df3.loc[0, "source"]
				query4 = f"""
				SELECT
					corresponds.source
				FROM corresponds 
				WHERE corresponds.scope = 'ecoscheme' AND corresponds.target = '{comb[1]}' 
				"""
				df4=pd.read_sql(query4, con=engine)
				if df4.empty:
					print(comb[1])
				val4= df4.loc[0, 'source']
				query5 = f"""
				SELECT
					correlations.value
				FROM correlations 
				WHERE correlations.scope = 'table1'  AND correlations.source = '{val3}' AND correlations.target =  '{val4}'
				"""
				df5=pd.read_sql(query5, con=engine)
				if len(df5) == 0:
					status_2 = 1
				else:
					value= df5.loc[0, 'value']
					if value == 0:
						status_2 = 0
						list_incomp.append(comb)
					else:
						status_2 = 1
	
	
	passed=None
	notes=''
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
			notes='Υπάρχουν ασυμβατότητες μεταξύ των Οικοσχημάτων: ' + s

	check_id = 9

	with engine.begin() as conn:
		conn.execute(text(f"""
			UPDATE application_checks
			SET checked_at = UTC_TIMESTAMP(),
				passed = {passed},
				notes = '{notes}'
			WHERE application_id = {id_key} AND check_id = {check_id}
		"""))
	return()


# crop_measures_incompatibility
def check_crop_measures_incompatibility(id_key):
	query1 = f"""
    SELECT
        applications.afm,
        applications.year,
        measures.code AS code
    FROM measures
    JOIN parcels ON parcels.id = measures.parcel_id
    JOIN applications ON applications.id = parcels.application_id
    WHERE applications.id = '{id_key}' AND measures.code LIKE 'Π3.70%'
    GROUP BY measures.code
    ORDER BY measures.code
    """
	df1 = pd.read_sql(query1, con=engine)
	if len(df1) == 0:
		status_1 = -1
	else:
		meas_list = df1["code"].tolist()
		if len(meas_list) == 1:
			status_1 = 0
		else:
			status_1 = 1
			comb_list = [list(c) for c in itertools.combinations(meas_list, 2)]
			list_incomp=[]
			for comb in comb_list:
				query3 = f"""
				SELECT
					corresponds.source
				FROM corresponds 
				WHERE corresponds.scope = 'measure' AND corresponds.target = '{comb[0]}' 
				"""
				df3=pd.read_sql(query3, con=engine)
				if df3.empty:
					print(comb[0])
				val3= df3.loc[0, "source"]
				query4 = f"""
				SELECT
					corresponds.source
				FROM corresponds 
				WHERE corresponds.scope = 'measure' AND corresponds.target = '{comb[1]}' 
				"""
				df4=pd.read_sql(query4, con=engine)
				if df4.empty:
					print(comb[1])
				val4= df4.loc[0, 'source']
				query5 = f"""
				SELECT
					correlations.value
				FROM correlations 
				WHERE correlations.scope = 'table4'  AND correlations.source = '{val3}' AND correlations.target =  '{val4}'
				"""
				df5=pd.read_sql(query5, con=engine)
				if len(df5) == 0:
					status_2 = 1
				else:
					value= df5.loc[0, 'value']
					if value == 0:
						status_2 = 0
						list_incomp.append(comb)
					else:
						status_2 = 1
	
	
	passed=None
	notes=''
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

	check_id = 10

	with engine.begin() as conn:
		conn.execute(text(f"""
			UPDATE application_checks
			SET checked_at = UTC_TIMESTAMP(),
				passed = {passed},
				notes = '{notes}'
			WHERE application_id = {id_key} AND check_id = {check_id}
		"""))

	return()


# crop_ecoschemes_measures_incompatibility

def check_crop_ecoschemes_measures_incompatibility(id_key):
	query1 = f"""
	SELECT
		applications.afm,
		applications.year,
		ecoschemes.code AS code
	FROM parcels
	JOIN applications ON applications.id = parcels.application_id
	JOIN ecoschemes ON ecoschemes.parcel_id = parcels.id
	WHERE applications.id = '{id_key}' AND parcels.is_seed_cultivation = 0
	GROUP BY applications.afm, ecoschemes.code
	ORDER BY ecoschemes.code
	"""
	df1 = pd.read_sql(query1, con=engine)
	query2 = f"""
    SELECT
        applications.afm,
        applications.year,
        measures.code AS code
    FROM measures
    JOIN parcels ON parcels.id = measures.parcel_id
    JOIN applications ON applications.id = parcels.application_id
    WHERE applications.id = '{id_key}' AND measures.code LIKE 'Π3.70%'
    GROUP BY measures.code
    ORDER BY measures.code
    """
	df2=pd.read_sql(query2, con=engine)

	if len(df1) == 0 or len(df2) == 0:
		status_1 = -1
	else:
		eco_list = df1["code"].tolist()
		meas_list = df2["code"].tolist()
		combinations = [list(pair) for pair in itertools.product(eco_list, meas_list)]
		status_1 = 1
		list_incomp=[]
		for comb in combinations:
			query3 = f"""
			SELECT
				corresponds.source
			FROM corresponds 
			WHERE corresponds.scope = 'ecoscheme' AND corresponds.target = '{comb[0]}' 
			"""
			df3=pd.read_sql(query3, con=engine)
			if df3.empty:
				print(comb[0])
			val3= df3.loc[0, "source"]
			query4 = f"""
			SELECT
				corresponds.source
			FROM corresponds 
			WHERE corresponds.scope = 'measure' AND corresponds.target = '{comb[1]}' 
			"""
			df4=pd.read_sql(query4, con=engine)
			if df4.empty:
				print(comb[1])
			val4= df4.loc[0, 'source']
			query5 = f"""
			SELECT
				correlations.value
			FROM correlations 
			WHERE correlations.scope = 'table3'  AND correlations.source = '{val3}' AND correlations.target =  '{val4}'
			"""
			df5=pd.read_sql(query5, con=engine)
			if len(df5) == 0:
				status_2 = 1
				print(df5)
				print(val3)
				print(val4)
			else:
				value= df5.loc[0, 'value']
				if value == 0:
					status_2 = 0
					list_incomp.append(comb)
				else:
					status_2 = 1
	
	
	passed=None
	notes=''
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

	check_id = 11

	with engine.begin() as conn:
		conn.execute(text(f"""
			UPDATE application_checks
			SET checked_at = UTC_TIMESTAMP(),
				passed = {passed},
				notes = '{notes}'
			WHERE application_id = {id_key} AND check_id = {check_id}
		"""))

	return()



	
# crop_connected

# crop_echoschemes_incompatibility

# crop_measures_incompatibility

# crop_ecoschemes_measures_incompatibility

# livestock_cross_period

# livestock_echoschemes_incompatibility

# livestock_measures_incompatibility

# livestock_ecoschemes_measures_incompatibility

query = f"""
SELECT
	applications.id
FROM applications
WHERE applications.year = '2025'
"""
df=pd.read_sql(query, con=engine)
if df.empty:
	exit()
else:
	ids = df['id']




for app_id in ids:
	# check_esap(app_id)
	# check_pasture_mmz(app_id)
	# check_corn_irrigation(app_id)
	# check_national_reserve(app_id)
	# check_young_farmers(app_id)
	check_application_ecoschemes(app_id)
	check_crop_echoschemes_incompatibility(app_id)
	check_crop_measures_incompatibility(app_id)
	check_crop_ecoschemes_measures_incompatibility(app_id)

print(len(ids))


