from sqlalchemy import text
import pandas as pd
from agriman.database import get_engine
import sys

# import check functions
from agriman.lib.checks.esap import check_esap
from agriman.lib.checks.pasture_mmz import check_pasture_mmz
from agriman.lib.checks.corn_irrigation import check_corn_irrigation
from agriman.lib.checks.national_reserve import check_national_reserve
from agriman.lib.checks.young_farmers import check_young_farmers
from agriman.lib.checks.application_ecoschemes import check_application_ecoschemes
from agriman.lib.checks.crop_ecoschemes_incompatibility import check_crop_ecoschemes_incompatibility
from agriman.lib.checks.crop_measures_incompatibility import check_crop_measures_incompatibility
from agriman.lib.checks.crop_ecoschemes_measures_incompatibility import check_crop_ecoschemes_measures_incompatibility
from agriman.lib.checks.livestock_echoschemes_incompatibility import check_livestock_echoschemes_incompatibility
from agriman.lib.checks.livestock_measures_incompatibility import check_livestock_measures_incompatibility
from agriman.lib.checks.livestock_ecoschemes_measures_incompatibility import check_livestock_ecoschemes_measures_incompatibility
from agriman.lib.checks.application_atak import check_application_atak
from agriman.lib.checks.crop_connected import check_crop_connected
from agriman.lib.checks.application_connected_documents import check_application_connected_documents


# Get the database engine
engine = get_engine()

# Get period_id from command line arguments
# Check for existance
if len(sys.argv) <= 1:
	print("You must give the period id")
	exit()
period_id = sys.argv[1]

# Fetch all application id's
query = text("""
  SELECT
    applications.id
  FROM applications
	WHERE period_id = :period_id
""")
df=pd.read_sql(query, con=engine, params={'period_id': period_id})
if df.empty:
	exit()
else:
	ids = df['id']

# Run the checks
print(f"Checking {len(ids)} applications ...")

for app_id in ids:
	check_esap(app_id)
	check_pasture_mmz(app_id)
	check_corn_irrigation(app_id)
	check_national_reserve(app_id)
	check_young_farmers(app_id)
	check_application_ecoschemes(app_id)
	check_crop_ecoschemes_incompatibility(app_id)
	check_crop_measures_incompatibility(app_id)
	check_crop_ecoschemes_measures_incompatibility(app_id)
	check_livestock_echoschemes_incompatibility(app_id)
	check_livestock_measures_incompatibility(app_id)
	check_livestock_ecoschemes_measures_incompatibility(app_id)
	check_application_atak(app_id)
	check_crop_connected(app_id)
	check_application_connected_documents(app_id)


