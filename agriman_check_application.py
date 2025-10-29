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
from agriman.lib.checks.application_sublease import check_application_sublease

# Get the database engine
engine = get_engine()

# Get application_id from command line args
# Check for existance
if len(sys.argv) <= 1:
	print("You must give the application id to be checked")
	exit()
app_id = sys.argv[1]

# Run the checks for given id
print(f"Checking application with id {app_id}...")

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
check_application_sublease(app_id)

