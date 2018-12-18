'''
just fill the
-month
-year
-munchkin_id (in 1P)
-client_id (in 1P)
-client_secret (in 1P)

and run it

it will write file like 'Jan_2018_leads.csv'
it also prints some messages while the script is running to let you know about the status
if you wanted more/different fields, check the other file I am sending along
 and alter the body variable (you want the rest name from the json)
'''
import sys
import requests
import pandas as pd
import time
import logging
from io import StringIO


def check_response(response, stage):
    if response.status_code != 200:
        print(stage + ' failed.')
        print('The response code is: ' + str(response.status_code))
        sys.exit(1)
    else:
        print(stage + ' passed.')


month = 'Oct'  # use three letters acronym in form like 'Jan', 'Feb' etc
year = 2018  # use number
munchkin_id = '566-GCC-428'
client_id = '015d7307-36ec-4f93-9eb2-e687814c0c72'
client_secret = 'TpVsSHm6xfMa2w94Qm1vmgoWlMaOQlh3'

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
            "startAt": months[month][0],
            "endAt": months[month][1]
        },
        "activityTypeIds":  [1, 2, 3]
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
df.to_csv(path_or_buf=('%s_%s_activities.csv' % (month, year)))
