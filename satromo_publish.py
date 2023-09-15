from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
import csv
import json
import os
import ee
import configuration as config
from collections import OrderedDict
import subprocess
import glob
from simple_settings import settings
from misc.run_utilities import RunType, determine_run_type
from loguru import logger


class Publisher:
    def __init__(self):
        self.drive = None
        self.rclone_file = 'rclone.conf'

    def initialize_gee_and_drive(self):
        """
        Initialize Google Earth Engine (GEE), RCLONE and Google Drive authentication.

        This function authenticates GEE and Google Drive either using a service account key file
        or GitHub secrets depending on the run type.

        Returns:
        None
        """

        scopes = ["https://www.googleapis.com/auth/drive"]
        if settings.run_type == RunType.DEV:
            # Initialize GEE and Google Drive using service account key file

            # Authenticate using the service account key file
            with open(settings.gdrive_secrets, "r") as f:
                service_account_key = json.load(f)

            # Authenticate Google Drive
            gauth = GoogleAuth()
            gauth.service_account_file = settings.gdrive_secrets
            gauth.service_account_email = service_account_key["client_email"]
            gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
                gauth.service_account_file, scopes=scopes
            )

        elif settings.run_type == RunType.INT:
            # Initialize GEE and Google Drive using GitHub secrets

            # Authenticate using the provided secrets from GitHub Actions
            gauth = GoogleAuth()
            google_client_secret = json.loads(
                os.environ.get('GOOGLE_CLIENT_SECRET'))
            gauth.service_account_email = google_client_secret["client_email"]
            gauth.service_account_file = "keyfile.json"
            with open(gauth.service_account_file, "w") as f:
                f.write(json.dumps(google_client_secret))
            gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
                gauth.service_account_file, scopes=scopes
            )

            # Write rclone config to a file
            rclone_config = os.environ.get('RCONF_SECRET')
            with open(settings.rclone_secrets, "w") as f:
                f.write(rclone_config)

            # Write GDRIVE Secrest config to a file
            google_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
            google_secret_file = "keyfile.json"
            with open(google_secret_file, "w") as f:
                f.write(google_secret)

            # Create mountpoint GDRIVE
            command = ["mkdir", settings.gdrive_mount]
            logger.info("Command: {}".format(command))
            result = subprocess.run(command, check=True)

            # GDRIVE Mount
            command = ["rclone", "mount", "--config", self.rclone_file,  # "--allow-other",
                       os.path.join(settings.gdrive_source), settings.gdrive_mount, "--vfs-cache-mode", "full"]

            logger.info("Command: {}".format(command))
            subprocess.Popen(command)

        # Create the Google Drive client
        self.drive = GoogleDrive(gauth)

        # Initialize EE
        credentials = ee.ServiceAccountCredentials(
            gauth.service_account_email, gauth.service_account_file
        )
        ee.Initialize(credentials)

        # Test EE initialization
        image = ee.Image("NASA/NASADEM_HGT/001")
        title = image.get("title").getInfo()
        if title == "NASADEM: NASA NASADEM Digital Elevation 30m":
            logger.info("GEE initialization successful")
        else:
            logger.error("GEE initialization FAILED")


    def download_and_delete_file(self, file):
        """
        DEV/local machine only Download a file from Google Drive and delete it afterwards.

        Parameters:
        file (GoogleDriveFile): Google Drive file object to download and delete.

        Returns:
        None
        """

        # Download the file to local machine
        file.GetContentFile(os.path.join(config.RESULTS, file["title"]))
        print(f"File {file['title']} downloaded.")

        # Delete the file
        file.Delete()
        print(f"File {file['title']} DELETED on Google Drive.")

    def move_files_with_rclone(self, source, destination):
        """
        Move files using the rclone command.

        Parameters:
        source (str): Source path of the files to be moved.
        destination (str): Destination path to move the files to.

        Returns:
        None
        """
        if settings.skip_s3:
            logger.warning("Skipping 'copy to s3' step")
            return

        # Run rclone command to move files
        # See hint https://forum.rclone.org/t/s3-rclone-v-1-52-0-or-after-permission-denied/21961/2
        source = os.path.abspath(source)
        command = ["rclone", "move", "--config", settings.rclone_secrets, "--s3-no-check-bucket",
                   source, destination]
        logger.debug(' '.join(command))
        subprocess.run(command, check=True)
        logger.info("SUCCESS: moved " + source + " to " + destination)

    def merge_files_with_gdal_warp(self, source):
        """
        Merge with GDAL

        Parameters:
        source (str): Source filename .

        Returns:
        None
        """

        if settings.os_name == "Windows":
            logger.warning("This is a Windows operating system, make sure you have enough disk space.")
        else:
            command = ["df", "-h"]
            logger.info('Command: {}'.format(command))
            result = subprocess.run(command, check=True,
                                    capture_output=True, text=True)
            logger.info(result)

        # Get the list of all quadrant files matching the pattern
        file_list = sorted(glob.glob(os.path.join(settings.gdrive_mount, source+"*.tif")))

        # under Windows Replace double backslashes with single backslashes in the file list
        if settings.os_name == "Windows":
            file_list = [filename.replace('\\\\', '\\') for filename in file_list]

        # Write the file names to _list.txt
        with open(source+"_list.txt", "w") as file:
            file.writelines([f"{filename}\n" for filename in file_list])

        # run gdal vrt
        command = ["gdalbuildvrt",
                   "-input_file_list", source + "_list.txt", source + ".vrt",
                   "--config", "GDAL_CACHEMAX", "9999",
                   "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
                   "--config", "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "YES",
                   ]
        logger.debug(' '.join(command))
        result = subprocess.run(command, check=True,
                                capture_output=True, text=True)
        logger.info(result)

        # run gdal translate
        command = ["gdalwarp",
                   # rename to source+"_merged.tif" when doing reprojection afterwards
                   source+".vrt", source+".tif",
                   "-of", "COG",
                   "-cutline", settings.buffer_file,
                   "-dstnodata", str(settings.NODATA),
                   "-srcnodata", str(settings.NODATA),
                   #"-co", "NUM_THREADS=ALL_CPUS",
                   "-co", "BIGTIFF=YES",
                   #"--config", "GDAL_CACHEMAX", "9999",
                   #"--config", "GDAL_NUM_THREADS", "ALL_CPUS",
                   "--config", "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "YES",
                   # otherwise use compress=DEFLATE
                   # https://kokoalberti.com/articles/geotiff-compression-optimization-guide/ and https://digital-geography.com/geotiff-compression-comparison/
                   "-co", "COMPRESS=LZW",
                   "-co", "PREDICTOR=2",
                   ]
        logger.debug(' '.join(command))
        result = subprocess.run(command, check=True,
                                capture_output=True, text=True)
        logger.info(result)

        # For Debugging uncomment  below
        # print("Standard Output:")
        # print(result.stdout)
        # print("Standard Error:")
        # print(result.stderr)

        print("SUCCESS: merged " + source+".tif")
        return (source+".tif")


    def extract_value_from_csv(self, filename, search_string, search_col, col_result):
        try:
            with open(filename, "r") as file:
                reader = csv.DictReader(file)

                for row in reader:
                    if row[search_col] == search_string:
                        return row[col_result]

                print(
                    f"Entry not found for '{search_string}' in column '{search_col}'")
        except FileNotFoundError:
            print("File not found.")

        return None


    def clean_up_gdrive(self, filename, item, product):
        """
        Deletes files in Google Drive that match the given filename.Writes Metadata of processing results

        Args:
            filename (str): The name of the file to be deleted.

        Returns:
            None
        """
        #  Find the file in Google Drive by its name
        file_list = self.drive.ListFile({
            "q": "title contains '"+filename+"' and trashed=false"
        }).GetList()

        # Check if the file is found
        if len(file_list) > 0:

            # Iterate through the files and delete them
            for file in file_list:

                # Get the current Task id
                file_on_drive = file['title']
                file_task_id = self.extract_value_from_csv(
                    settings.gee_running_tasks, file_on_drive.replace(".tif", ""), "Filename", "Task ID")

                # Check task status
                file_task_status = ee.data.getTaskStatus(file_task_id)[0]

                # Get the product and item
                file_product, file_item = self.extract_product_and_item(
                    file_task_status['description'])

                # Delete the file
                file.Delete()
                print(f"File {file['title']} DELETED on Google Drive.")

                # Add DATA GEE PROCESSING info to stats
                self.write_file(file_task_status, settings.gee_completed_tasks)

                # Remove the line from the RUNNING tasks file
                self.delete_line_in_file(settings.gee_running_tasks, file_task_id)

            # Add DATA GEE PROCESSING info to Metadata of item,
            self.write_file_meta(file_task_status, os.path.join(settings.processing_dir, item + ".csv"))

            # Get the filename metadata
            metadata = self.read_file_meta(os.path.join(settings.processing_dir, filename+".csv"))

            # Move Description of item to destination DIR
            self.move_files_with_rclone(os.path.join(settings.processing_dir, item+".csv"),
                                        os.path.join(settings.s3_destination, product, metadata['Item']))

            # Move Metadata of item to destination DIR, only for  RAW data products, assuming we take always the first
            pattern = f"*{metadata['Item']}*_properties_*.json"
            files_matching_pattern = glob.glob(
                os.path.join(settings.processing_dir, pattern))
            if files_matching_pattern:
                destination_dir = os.path.join(
                    settings.s3_destination, product, metadata['Item'])
                for file_to_move in files_matching_pattern:
                    self.move_files_with_rclone(file_to_move, destination_dir)

            # Update Status in RUNNING tasks file
            self.replace_running_with_complete(settings.last_product_updates, file_product)

            # Clean up GDAL temporary files

            # VRT file, Pattern for .vrt files
            vrt_pattern = f"*{metadata['Item']}*.vrt"
            vrt_files = glob.glob(vrt_pattern)
            [os.remove(file_path)
             for file_path in vrt_files if os.path.exists(file_path)]

            # Pattern for _list.txt files
            list_txt_pattern = f"*{metadata['Item']}*_list.txt"
            list_files = glob.glob(list_txt_pattern)
            [os.remove(file_path)
             for file_path in list_files if os.path.exists(file_path)]

        return


    def write_file(self, input_dict, output_file):
        """
        Write a dictionary to a CSV file. If the file exists, the data is appended
        to it. If the file does not exist, a new file is created with a header.

        Parameters:
        input_dict (dict): Dictionary to be written to file.
        output_file (str): Path of the output file.

        Returns:
        None
        """
        append_or_write = "a" if os.path.isfile(output_file) else "w"
        with open(output_file, append_or_write, encoding="utf-8", newline='') as f:
            dict_writer = csv.DictWriter(f, fieldnames=list(input_dict.keys()),
                                         delimiter=",", quotechar='"',
                                         lineterminator="\n")
            if append_or_write == "w":
                dict_writer.writeheader()
            dict_writer.writerow(input_dict)
        return


    def delete_line_in_file(self, filepath, stringtoremove):
        """
        Delete lines containing a specific string from a file.

        Parameters:
        filepath (str): Path of the file to modify.
        stringtoremove (str): String to search for and remove from the file.

        Returns:
        None
        """
        with open(filepath, "r+") as file:
            lines = file.readlines()
            file.seek(0)
            file.truncate()
            for line in lines:
                if stringtoremove not in line.strip() and line.strip():
                    file.write(line)
                elif not line.strip():
                    file.write("\n")

    def write_file_meta(self, input_dict, output_file):
        """
        Read the existing CSV file, append the input dictionary, and export it as a new CSV file.

        Parameters:
        input_dict (dict): Dictionary to be appended to the CSV file.
        output_file (str): Path of the output CSV file.

        Returns:
        None
        """
        existing_data = OrderedDict()
        if os.path.isfile(output_file):
            with open(output_file, "r", encoding="utf-8", newline='') as f:
                reader = csv.reader(f)
                existing_data = OrderedDict(zip(next(reader), next(reader)))

        existing_data.update(input_dict)

        with open(output_file, "w", encoding="utf-8", newline='') as f:
            writer = csv.writer(f, delimiter=",", quotechar='"',
                                lineterminator="\n")

            writer.writerow(list(existing_data.keys()))
            writer.writerow(list(existing_data.values()))

    def read_file_meta(self, input_file):
        """
        Read the existing CSV file

        Parameters:
        input_file (str): Path of the output CSV file.

        Returns:
        None
        """
        existing_data = OrderedDict()
        if os.path.isfile(input_file):
            with open(input_file, "r", encoding="utf-8", newline='') as f:
                reader = csv.reader(f)
                existing_data = OrderedDict(zip(next(reader), next(reader)))

        return existing_data


    def extract_product_and_item(self, task_description):
        """
        Extract the product and item information from a task description.

        Parameters:
        task_description (str): Description of the task containing product and item information.

        Returns:
        tuple: A tuple containing the extracted product and item information.
        """
        product_start_index = task_description.index('P:') + 2
        product_end_index = task_description.index(' I:')
        product = task_description[product_start_index:product_end_index]

        item_start_index = task_description.index('I:') + 2
        item = task_description[item_start_index:]

        return product, item

    def replace_running_with_complete(self, input_file, item):
        """
        Replace 'RUNNING' with 'complete' in the specific item line of an input file.

        Parameters:
        input_file (str): Path to the input file.
        item (str): Item to identify the line to be modified.

        Returns:
        None
        """
        output_lines = []
        with open(input_file, 'r') as f:
            for line in f:
                if line.startswith(item):
                    line = line.replace('RUNNING', 'complete')
                output_lines.append(line)

        with open(input_file, 'w') as f:
            f.writelines(output_lines)

    def run(self):
        # Test if we are on Local DEV Run or if we are on PROD
        determine_run_type()

        # Authenticate with GEE and GDRIVE
        self.initialize_gee_and_drive()

        # empty temp files on GDrive
        file_list = self.drive.ListFile({'q': "trashed=true"}).GetList()
        for file in file_list:
            file.Delete()
            print('GDRIVE TRASH: Deleted file: %s' % file['title'])

        # Read the status file
        with open(settings.gee_running_tasks, "r") as f:
            lines = f.readlines()

        # Get the unique filename
        unique_filenames = set()

        for line in lines[1:]:  # Start from the second line
            _, filename = line.strip().split(',')
            # Take the part before "quadrant"
            filename = filename.split('quadrant')[0]
            unique_filenames.add(filename.strip())

        unique_filenames = list(unique_filenames)

        # Check  if each quandrant is complete then process
        # Iterate over unique filenames
        for filename in unique_filenames:

            # Keep track of completion status
            all_completed = True
            # You need to change this if we have more than 4 quadrants
            for quadrant_num in range(1, 5):
                # Construct the filename with the quadrant
                full_filename = filename + "quadrant" + str(quadrant_num)

                # Find the corresponding task ID in the lines list
                task_id = None
                for line in lines[1:]:
                    if full_filename in line:
                        task_id = line.strip().split(",")[0]
                        break

                if task_id:
                    # Check task status
                    task_status = ee.data.getTaskStatus(task_id)[0]

                if task_status["state"] != "COMPLETED":
                    # Task is not completed
                    all_completed = False
                    logger.warning(f"{full_filename} - {task_status['state']}")

            # Check overall completion status
            if all_completed:
                # if run_type == 2:
                # local machine run
                # Download DATA
                # breakpoint()  # TODO add local processor
                # download_and_delete_file(filename)
                # else:
                print(filename+" is ready to process")

                # Get the product and item
                product, item = self.extract_product_and_item(task_status['description'])

                # Get the metadata
                metadata = self.read_file_meta(os.path.join(settings.processing_dir, filename + ".csv"))

                # merge files
                file_merged = self.merge_files_with_gdal_warp(filename)

                # move file to Destination: in case reproejction is done here: move file_reprojected
                self.move_files_with_rclone(
                    file_merged, os.path.join(settings.s3_destination, product, metadata['Item']))

                # clean up GDrive and local drive
                # os.remove(file_merged)
                self.clean_up_gdrive(filename, item, product)

            else:
                logger.info("{} is NOT ready to process".format(filename))

        # Last step
        if settings.run_type == RunType.INT:
            # Remove the key file so It wont be commited
            logger.info('removing key files')
            os.remove("keyfile.json")
            os.remove(settings.rclone_secrets)

        # empty temp files on GDrive
        file_list = self.drive.ListFile({'q': "trashed=true"}).GetList()
        for file in file_list:
            file.Delete()
            logger.info('GDRIVE TRASH: Deleted file: %s' % file['title'])

        logger.info("PUBLISH Process done.")


if __name__ == "__main__":
    publisher = Publisher()
    publisher.run()
