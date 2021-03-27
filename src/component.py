import sys
import requests
import time
import logging
import os
from keboola import docker
from datetime import datetime, timedelta
import json
import logging_gelf.handlers
import logging_gelf.formatters  # noqa


sys.tracebacklimit = 0

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger()
logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
    host=os.getenv('KBC_LOGGER_ADDR'),
    port=int(os.getenv('KBC_LOGGER_PORT'))
)
logging_gelf_handler.setFormatter(
    logging_gelf.formatters.GELFFormatter(null_character=True))
logger.addHandler(logging_gelf_handler)

# removes the initial stdout logging
logger.removeHandler(logger.handlers[0])


# Disabling list of libraries you want to output in the logger
disable_libraries = [
    'urllib3',
    'requests'
]
for library in disable_libraries:
    logging.getLogger(library).disabled = True

# Destination to fetch and output files and tables
DEFAULT_TABLE_INPUT = "/data/in/tables/"
DEFAULT_FILE_INPUT = "/data/in/files/"

DEFAULT_FILE_DESTINATION = "/data/out/files/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"

APP_VERSION = '1.3.6'
KEY_DEBUG = 'debug'


class Component():
    def __init__(self, debug=False):

        # override debug from config
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
            logging.info('Running version %s', APP_VERSION)
            logging.info('Loading configuration...')

    def run(self):
        '''
        Main execution code
        '''

        # Access the supplied rules
        cfg = docker.Config('/data/')
        params = cfg.get_parameters()

        # Validating user inputs
        self.validate_user_parameters(params)

        # Read the parameters
        # Credentials parameters
        client_id = params.get("#client_id")
        munchkin_id = params.get("munchkinid")
        client_secret = params.get("#client_secret")

        # Endpoint parameters
        endpoint = params.get("endpoint")
        dayspan_created = params.get("dayspan_created")
        month_year_created = params.get("month/year_created")
        month_year_updated = params.get("month/year_updated")
        dayspan_updated = params.get("dayspan_updated")
        self.desired_activities_tmp = params.get("desired_activities")
        self.desired_activities = [i.strip()
                                   for i in self.desired_activities_tmp.split(",")]
        self.fields_str_tmp = params.get("desired_fields")
        self.fields_str = [i.strip() for i in self.fields_str_tmp.split(",")]

        # Outputing log if parameters are configured
        logging.info("Endpoint: %s" % endpoint)
        logging.info("Dayspan updated: %s" %
                     dayspan_updated) if dayspan_updated else ''
        logging.info("Dayspan created: %s" %
                     dayspan_created) if dayspan_created else ''
        logging.info("Desired activities: %s" %
                     str(self.desired_activities)) if self.desired_activities_tmp else ''
        logging.info("Month/Year updated: %s" %
                     month_year_updated) if month_year_updated else ''
        logging.info("Month/Year created: %s" %
                     month_year_created) if month_year_created else ''
        logging.info("Desired fields: %s" %
                     str(self.fields_str)) if self.fields_str_tmp else ''

        # Request base Url & Authenticating
        self.BASE_URL = f'https://{munchkin_id}.mktorest.com'
        self.access_token = self.authenticate(client_id, client_secret)

        # Request parameters based on user inputs
        CREATED_DATE, start_created, end_created = self.create_date_ranges(
            dayspan_created, month_year_created, 'Created')
        UPDATED_DATE, start_updated, end_updated = self.create_date_ranges(
            dayspan_updated, month_year_updated, 'Updated')

        date_obj = {
            'created_date_bool': CREATED_DATE,
            'start_created_date': start_created,
            'end_created_date': end_created,
            'updated_date_bool': UPDATED_DATE,
            'start_updated_date': start_updated,
            'end_updated_date': end_updated
        }

        # Endpoint Request
        self.fetch_endpoint(endpoint.lower(), date_obj)

    def validate_user_parameters(self, params):
        # 1 - check if the configuration is empty
        if not params or params == {}:
            logging.error('Please configure your component.')
            sys.exit(1)

        # 2 - Check if all the credentials are entered
        if not params.get('#client_id') or not params.get('munchkinid') or not params.get('#client_secret'):
            logging.error(
                "Credentials are missing: [Client ID], [Munchkin ID], [Client Secret]")
            sys.exit(1)

        # 3 - ensure the endpoints are supported
        if params.get('endpoint') not in ('Activities', 'Leads'):
            logging.error('Specified endpoint is not supported.')
            sys.exit(1)

        # 4 - when endpoint leads is selected, desired fields cannot be empty
        fields_str_tmp = params.get('desired_fields')
        fields_str = [i.strip() for i in fields_str_tmp.split(",")
                      ] if fields_str_tmp else ''
        if params.get('endpoint') == 'Leads' and len(fields_str) == 0:
            logging.error(
                "Please specify [Desired Fields] when endpoint [Leads] is selected.")
            sys.exit(1)

    def check_response(self, response, stage):
        if response.status_code != 200:
            logging.error(f'[{response.status_code}] - {stage} failed.')
            sys.exit(1)
        else:
            logging.info(stage)

    def authenticate(self, client_id, client_secret):

        auth_url = f'{self.BASE_URL}/identity/oauth/token'
        params = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials'
        }

        response = requests.get(url=auth_url, params=params)
        self.check_response(response, 'Fetching access token')

        return response.json()['access_token']

    def get_request(self, url, params=None):

        try:
            response = requests.get(url, params=params)
        except Exception as err:
            logging.error(f'Error occured: {err}')
            sys.exit(1)

        return response

    def post_request(self, url, params=None, body=None):

        try:
            response = requests.post(url, params=params, json=body)
        except Exception as err:
            logging.error(f'Error occured: {err}')
            sys.exit(1)

        return response

    def create_date_ranges(self, dayspan, month_year, date_type):
        '''
        Created Filter
        Determine whether we want to get data from apst X days or from a specific month/year
        '''
        # Return parameters
        CREATED_DATE = False
        start_date = ''
        end_date = ''

        if dayspan != '':
            # Using the dayspan value regardless if the created value is empty or not
            CREATED_DATE = True
            start_date = str(
                (datetime.utcnow() - timedelta(days=int(dayspan))).date())
            end_date = str(datetime.utcnow().date())

            # Disregarding Created value is not empty
            if month_year != '':
                logging.info(f'Disregarding the <Month/Year for      \'{date_type}\'> parameter, taking into consideration only \
                     the <How many days back you want to go with \'{date_type}\'?> parameter''Disregrading the <Month/\
                    Year ')

        # when dayspan variable is not specified
        else:
            if month_year != '':
                CREATED_DATE = True
                month = month_year[:3].lower()
                year = int(month_year[4:])
                if year % 4 == 0 and year % 400 != 0:
                    feb_length = '29'
                else:
                    feb_length = '28'
                year = str(year)
                months = {
                    'jan': [year + "-01-01T00:00:00Z", year + "-01-31T23:59:59Z"],
                    'feb': [year + "-02-01T00:00:00Z", year + "-02-" + feb_length + "T23:59:59Z"],
                    'mar': [year + "-03-01T00:00:00Z", year + "-03-31T23:59:59Z"],
                    'apr': [year + "-04-01T00:00:00Z", year + "-04-30T23:59:59Z"],
                    'may': [year + "-05-01T00:00:00Z", year + "-05-31T23:59:59Z"],
                    'jun': [year + "-06-01T00:00:00Z", year + "-06-30T23:59:59Z"],
                    'jul': [year + "-07-01T00:00:00Z", year + "-07-31T23:59:59Z"],
                    'aug': [year + "-08-01T00:00:00Z", year + "-08-31T23:59:59Z"],
                    'sep': [year + "-09-01T00:00:00Z", year + "-09-30T23:59:59Z"],
                    'oct': [year + "-10-01T00:00:00Z", year + "-10-31T23:59:59Z"],
                    'nov': [year + "-11-01T00:00:00Z", year + "-11-30T23:59:59Z"],
                    'dec': [year + "-12-01T00:00:00Z", year + "-12-31T23:59:59Z"]
                }
                start_date = months[month][0][:10]
                end_date = months[month][1][:10]

            else:
                CREATED_DATE = False
                logging.info(f'{date_type} date is not provided.')

        return CREATED_DATE, start_date, end_date

    def fetch_endpoint(self, endpoint, date_obj):
        '''
        Endpoint: Activities
        '''

        # Request parameters
        request_url = f'{self.BASE_URL}/bulk/v1/{endpoint}/export'
        request_param = {
            'access_token': self.access_token
        }
        request_body = {
            'format': 'CSV'
        }

        # setting up request parameters depending on endpoint
        if endpoint == 'activities':
            # Create date parameters
            if not date_obj['created_date_bool']:
                logging.error(
                    'The Activities endpoint requires Created Date interval.')
                sys.exit(1)
            else:
                request_body['filter'] = {}
                created_at = {
                    'startAt': date_obj['start_created_date'],
                    'endAt': date_obj['end_created_date']
                }
                request_body['filter']['createdAt'] = created_at

            # Update date parameters
            if date_obj['updated_date_bool']:
                updated_at = {
                    'startAt': date_obj['start_updated_date'],
                    'endAt': date_obj['end_updated_date']
                }
                request_body['filter']['updatedAt'] = updated_at

            # activities specificiations
            if len(self.desired_activities) > 0:
                request_body['filter']['activityTypeIds'] = self.desired_activities

        elif endpoint == 'leads':
            request_body['fields'] = self.fields_str

            # Filter parameters
            if not date_obj['updated_date_bool'] and not date_obj['created_date_bool']:
                logging.error(
                    'The Leads endpoint requries either Created or Updated parameter.')
                sys.exit(1)

            else:
                request_body['filter'] = {}

            # Update paramaters
            if date_obj['updated_date_bool']:
                updated_at = {
                    'startAt': date_obj['start_updated_date'],
                    'endAt': date_obj['end_updated_date']
                }
                request_body['filter']['updatedAt'] = updated_at

            # Create parameters
            if date_obj['created_date_bool']:
                created_at = {
                    'startAt': date_obj['start_created_date'],
                    'endAt': date_obj['end_created_date']
                }
                request_body['filter']['createdAt'] = created_at

        # 1 - Create exports
        export_id = self.create_mkto_export(
            request_url, request_param, request_body)

        # 2 - Enqueue export
        self.enqueue_mkto_export(request_url, request_param, export_id)

        # 3 - loop while waiting for the report to be ready
        ready_bool = False
        while not ready_bool:
            ready_bool = self.check_mkto_export_status(
                request_url, request_param, export_id)

        # 4 - Outputing the file
        self.output_mkt_export(request_url, request_param, export_id, endpoint)

    def create_mkto_export(self, request_url, request_param, request_body):

        export_url = f'{request_url}/create.json'
        '''create_export = requests.post(
            url=export_url, params=request_param, json=request_body)'''
        create_export = self.post_request(
            export_url, request_param, request_body)
        self.check_response(create_export, 'Creating export')

        if not create_export.json()['success']:
            logging.error(
                f'Creating export was not successful; Errors: {create_export.json()["errors"]}')
            sys.exit(1)

        export_id = create_export.json()['result'][0]['exportId']
        logging.info(f'Export ID: [{export_id}]')

        return export_id

    def enqueue_mkto_export(self, request_url, request_param, export_id):

        enqueue_url = f'{request_url}/{export_id}/enqueue.json'
        '''enqueue_export = requests.post(url=enqueue_url, params=request_param)'''
        enqueue_export = self.post_request(
            enqueue_url, request_param, body=None)
        self.check_response(enqueue_export, 'Enqueuing export')

    def check_mkto_export_status(self, request_url, request_param, export_id):

        time.sleep(60)

        ready_bool = False
        status_url = f'{request_url}/{export_id}/status.json'
        '''status_export = requests.get(url=status_url, params=request_param)'''
        status_export = self.get_request(status_url, params=request_param)
        self.check_response(status_export, 'Standing by for export status')

        try:
            if status_export.json()['result'][0]['status'] == 'Completed':
                ready_bool = True
        except KeyError:
            logging.error("There was a problem when obtaining the status of the export.\
            Please try rerunning the configuration as the API sometimes behaves unpredictably.")
            logging.error(f'Response: {status_export.json()}')
            sys.exit(1)
        except Exception as e:
            logging.error(e)
            sys.exit(1)

        return ready_bool

    def output_mkt_export(self, request_url, request_param, export_id, endpoint):

        # Output file destination
        output_file_name = endpoint.capitalize() + '_bulk.csv'
        output_file_destination = DEFAULT_TABLE_DESTINATION + output_file_name

        # Output file request parameter
        output_url = f'{request_url}/{export_id}/file.json'

        '''response = requests.get(request_url, params=request_param)'''
        response = self.get_request(output_url, request_param)

        # Output file
        csv_file = open(output_file_destination, 'wb')
        csv_file.write(response.content)
        csv_file.close()

        # Outputting manifest if there is data
        if len(list(response.content)) == 0:
            logging.info(
                'The export from the API reached state Completed, but no data were transferred from the API.')
            os.remove(output_file_destination)

        else:
            pk = ['marketoGUID'] if endpoint == 'activities' else ['id']
            self.save_manifest(
                file_name=output_file_name, primary_keys=pk)

            logging.info(f'{endpoint} exported.')

    def save_manifest(self, file_name, primary_keys):
        """
        Dummy function for returning manifest
        """

        file = '/data/out/tables/' + file_name + ".manifest"

        logging.info("Manifest output: {0}".format(file))

        manifest = {
            'destination': '',
            'incremental': True,
            'primary_key': primary_keys
        }

        try:
            with open(file, 'w') as file_out:
                json.dump(manifest, file_out)
                logging.info(
                    "Output manifest file ({0}) produced.".format(file))
        except Exception as e:
            logging.error("Could not produce output file manifest.")
            logging.error(e)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(2)
