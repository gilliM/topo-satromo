import datetime
from misc.run_utilities import determine_run_type, RunType
from simple_settings import settings
from loguru import logger
import json
from pydrive.auth import GoogleAuth
from oauth2client.service_account import ServiceAccountCredentials
from .ndvimax_processor import NdviMaxProcessor
from .l2a_processor import L2AProcessor
import ee

class Processor:
    def __init__(self):
        # Test if we are on Local DEV Run or if we are on PROD
        determine_run_type()

        # Get current date
        #
        # For debugging
        #current_date_str = "2023-09-11"
        #print("*****************************")
        #print("")
        #print("using a manual set Date: "+current_date_str)
        #print("*****************************")
        #print("")
        current_date_str = datetime.datetime.today().strftime('%Y-%m-%d')
        settings.configure(current_date_str=current_date_str)


    def initialize_gee_and_drive(self):
        """
        Initializes Google Earth Engine (GEE) and Google Drive based on the run type.

        If the run type is 2, initializes GEE and authenticates using the service account key file.
        If the run type is 1, initializes GEE and authenticates using secrets from GitHub Action.

        Prints a success or failure message after initializing GEE.

        Note: This function assumes the required credentials and scopes are properly set.

        Returns:
            None
        """
        # Set scopes for Google Drive
        scopes = ["https://www.googleapis.com/auth/drive"]

        if settings.run_type == RunType.DEV:
            # Initialize GEE and authenticate using the service account key file

            # Read the service account key file
            with open(settings.gdrive_secrets, "r") as f:
                data = json.load(f)

            # Authenticate with Google using the service account key file
            gauth = GoogleAuth()
            gauth.service_account_file = settings.gdrive_secrets
            gauth.service_account_email = data["client_email"]
            gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
                gauth.service_account_file, scopes=scopes
            )
        elif settings.run_type == RunType.INT:
            # Run other code using secrets from GitHub Action
            # This script is running on GitHub
            gauth = GoogleAuth()
            google_client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
            google_client_secret = json.loads(google_client_secret)
            gauth.service_account_email = google_client_secret["client_email"]
            google_client_secret_str = json.dumps(google_client_secret)

            # Write the JSON string to a temporary key file
            gauth.service_account_file = "keyfile.json"
            with open(gauth.service_account_file, "w") as f:
                f.write(google_client_secret_str)

            gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
                gauth.service_account_file, scopes=scopes
            )
        else:
            raise BrokenPipeError('Run Type Unkwown')

        # Initialize Google Earth Engine
        credentials = ee.ServiceAccountCredentials(
            gauth.service_account_email, gauth.service_account_file
        )
        ee.Initialize(credentials)

        # Test if GEE initialization is successful
        image = ee.Image("NASA/NASADEM_HGT/001")
        title = image.get("title").getInfo()

        if title == "NASADEM: NASA NASADEM Digital Elevation 30m":
            logger.info("GEE initialization successful")
        else:
            logger.error("GEE initialization FAILED")

    def run(self):
        # Authenticate with GEE and GDRIVE
        self.initialize_gee_and_drive()

        # Generate PRODUCTS
        # NDVI MAX
        ndvi_processor = NdviMaxProcessor()
        result = ndvi_processor.run()
        logger.info("Result:", result)

        # S2_L2A
        l2a_processor = L2AProcessor()
        result = l2a_processor.run()
        logger.info("Result:", result)
        logger.info("Processing done!")


if __name__ == "__main__":
    processor = Processor()
    processor.run()

