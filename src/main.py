import sys
import requests
import pandas as pd
import time
import logging
from io import StringIO
import os
from keboola import docker
from datetime import datetime, timedelta

# Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

sys.tracebacklimit = None

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

# Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()

client_id = cfg.get_parameters()["#client_id"]
munchkin_id = cfg.get_parameters()["#munchkin_id"]
client_secret = cfg.get_parameters()["#client_secret"]
dayspan = cfg.get_parameters()["dayspan"]
desired_activities_tmp = cfg.get_parameters()["desired_activities"]
desired_activities = [i.strip() for i in desired_activities_tmp.split(",")]

if dayspan == '':
    month = cfg.get_parameters()["month/year"][:3]
    year = cfg.get_parameters()["month/year"][4:]
    if year % 4 == 0 and year % 400 != 0:
        feb_length = '29'
    else:
        feb_length = '28'
    year = str(year)
    months = {
        'Jan': [year + "-01-01T00:00:00Z", year + "-01-31T23:59:59Z"],
        'Feb': [year + "-02-01T00:00:00Z", year + "-02-" + feb_length + "T23:59:59Z"],
        'Mar': [year + "-03-01T00:00:00Z", year + "-03-31T23:59:59Z"],
        'Apr': [year + "-04-01T00:00:00Z", year + "-04-30T23:59:59Z"],
        'May': [year + "-05-01T00:00:00Z", year + "-05-31T23:59:59Z"],
        'Jun': [year + "-06-01T00:00:00Z", year + "-06-30T23:59:59Z"],
        'Jul': [year + "-07-01T00:00:00Z", year + "-07-31T23:59:59Z"],
        'Aug': [year + "-08-01T00:00:00Z", year + "-08-31T23:59:59Z"],
        'Sep': [year + "-09-01T00:00:00Z", year + "-09-30T23:59:59Z"],
        'Oct': [year + "-10-01T00:00:00Z", year + "-10-31T23:59:59Z"],
        'Nov': [year + "-11-01T00:00:00Z", year + "-11-30T23:59:59Z"],
        'Dec': [year + "-12-01T00:00:00Z", year + "-12-31T23:59:59Z"]
    }
    start = months[month][0]
    end = months[month][1]
else:
    start = str((datetime.utcnow() - timedelta(days=int(dayspan)))
                .date())
    end = str(datetime.utcnow().date())

logging.info("params read")

# Destination to fetch and output files and tables
DEFAULT_TABLE_INPUT = "/data/in/tables/"
DEFAULT_FILE_INPUT = "/data/in/files/"

DEFAULT_FILE_DESTINATION = "/data/out/files/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"


def check_response(response, stage):
    if response.status_code != 200:
        print(stage + ' failed.')
        print('The response code is: ' + str(response.status_code))
        sys.exit(1)
    else:
        print(stage + ' passed.')


parameters = {'munchkin_id': munchkin_id,
              'client_id': client_id,
              'client_secret': client_secret}

parameters_1 = {'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'client_credentials'}


resp = requests.get(
    url='https://566-GCC-428.mktorest.com/identity/oauth/token', params=parameters_1)
check_response(resp, 'Obtaining access token')

access_token = resp.json()['access_token']


parameters_2 = {'access_token': access_token}

body = {
    # "fields": [
    #     "firstName",
    #     "lastName",
    #     "id",
    #     "createdAt",
    #     "company",
    #     "email",
    #     "phone",
    #     "title",
    #     "updatedAt",
    #     "leadSource",
    #     "acquisitionProgramId",
    #     "C_Lead_Source_Original__c",
    #     "Campaign__c"

    # ],
    "format": "CSV",
    # "columnHeaderNames": {
    #     "id": "lead_id",
    #     "createdAt": "created_at",
    #     "company": "company",
    #     "email": "email",
    #     "phone": "phone",
    #     "title": "job_title",
    #     "updatedAt": "updated_at",
    #     "leadSource": "lead_source",
    #     "acquisitionProgramId": "acquisition_program_id",
    #     "C_Lead_Source_Original__c": "original_lead_source",
    #     "Campaign__c": "primary_campaign",
    #     "firstName": "first_name",
    #     "lastName": "last_name"
    # },
    "filter": {
        "createdAt": {
            "startAt": start,
            "endAt": end
        },
        "activityTypeIds":  desired_activities
    }
}

create_export = requests.post('https://566-GCC-428.mktorest.com/bulk/v1/activities/export/create.json',
                              params=parameters_2, json=body)

check_response(create_export, 'Creating export')
if not create_export.json()['success']:
    logging.info('Creating export was not successfull.')
    logging.info('Errors:')
    logging.info(create_export.json()['errors'])

export_id = create_export.json()['result'][0]['exportId']

enqueue_export = requests.post('https://566-GCC-428.mktorest.com/bulk/v1/activities/export/' +
                               export_id + '/enqueue.json',
                               params=parameters_2)

check_response(enqueue_export, 'Enqueuing export')

time.sleep(60)
status_export = requests.get('https://566-GCC-428.mktorest.com/bulk/v1/activities/export/' +
                             export_id + '/status.json',
                             params=parameters_2)

check_response(status_export, 'Getting status of the export')

while status_export.json()['result'][0]['status'] != 'Completed':
    print('W8 m8')
    time.sleep(60)
    status_export = requests.get('https://566-GCC-428.mktorest.com/bulk/v1/activities/export/' +
                                 export_id + '/status.json',
                                 params=parameters_2)
    check_response(status_export, 'Getting status of the export')

file_export = requests.get('https://566-GCC-428.mktorest.com/bulk/v1/activities/export/' +
                           export_id + '/file.json',
                           params=parameters_2)

check_response(file_export, 'File_export')


s = str(file_export.content, 'utf-8')

data = StringIO(s)

df = pd.read_csv(data)
df.to_csv(path_or_buf=(DEFAULT_TABLE_DESTINATION + '%s_%s_activities.csv' % (start, end)),
          index=False)
