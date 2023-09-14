import os
import yaml
from simple_settings import settings
from misc.logger import logger
import platform
from enum import Enum

current_directory = os.path.dirname(os.path.abspath(__file__))
main_directory = os.path.join(current_directory, '..')

def set_config():
    config_file = settings.config_file + '.yaml'
    file_path = os.path.abspath(os.path.join(main_directory, 'configuration', config_file))
    with open(file_path, 'r') as src:
        data = yaml.load(src, Loader=yaml.SafeLoader)

    settings.configure(config=data)
    settings.configure(gee_running_tasks=os.path.join(main_directory, *data['GEE_RUNNING_TASKS']))
    settings.configure(gee_completed_tasks=os.path.join(main_directory, *data['GEE_COMPLETED_TASKS']))
    settings.configure(processing_dir=os.path.join(main_directory, data['PROCESSING_DIR']))
    settings.configure(last_product_updates=os.path.join(main_directory, *data['LAST_PRODUCT_UPDATES']))

    settings.configure(gdrive_secrets=os.path.join(main_directory, 'secrets', settings.config['GDRIVE_SECRETS']))
    # Get the operating system name
    os_name = platform.system()
    settings.configure(os_name=os_name)


class RunType(Enum):
    INT = 1
    DEV = 2


def determine_run_type():
    """
    Determines the run type based on the existence of the SECRET on the local machine file.

    If the file `config.GDRIVE_SECRETS` exists, sets the run type to 2 (DEV) and prints a corresponding message.
    Otherwise, sets the run type to 1 (PROD) and prints a corresponding message.
    """

    if os.path.exists(settings.gdrive_secrets):
        settings.configure(run_type=RunType.DEV)
    else:
        settings.configure(run_type=RunType.INT)

    logger.info("\nType {} run PROCESSOR: We are on {}".
                format(settings.run_type.value, settings.run_type))

    logger.debug('determine_run_type over')

