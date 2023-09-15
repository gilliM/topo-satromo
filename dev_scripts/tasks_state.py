import os
import json
import yaml
import ee
from pydrive.auth import GoogleAuth
from oauth2client.service_account import ServiceAccountCredentials

print(os.getcwd())
with open(os.path.join('..', 'configuration', 'base_config.yaml')) as src:
    config = yaml.load(src, Loader=yaml.FullLoader)

gdrive_secrets = os.path.join('..', 'secrets', config['GDRIVE_SECRETS'])

with open(gdrive_secrets, "r") as f:
    data = json.load(f)


credentials = ee.ServiceAccountCredentials(
    data["client_email"], gdrive_secrets
)
ee.Initialize(credentials)


tasks = ee.data.listOperations()
print('number of tasks: {}'.format(len(tasks)))

def print_stats(tasks, attribute):
    hist_dict = dict()
    for task in tasks:
        try:
            cvalue = task['metadata'][attribute]
        except KeyError:
            continue
        if cvalue in hist_dict:
            hist_dict[cvalue] += 1
        else:
            hist_dict[cvalue] = 1
    print('{}: {}'.format(attribute, hist_dict))

print_stats(tasks, 'type')
print_stats(tasks, 'state')
print_stats(tasks, 'progress')
