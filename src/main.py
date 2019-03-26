'''
code is a hot fix, not a finished work
'''

import sys
import requests
import time
import logging
import os
from keboola import docker
from datetime import datetime, timedelta
import subprocess
import json

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

# Read the parameters
client_id = cfg.get_parameters()["#client_id"]
munchkin_id = cfg.get_parameters()["#munchkin_id"]
client_secret = cfg.get_parameters()["#client_secret"]
dayspan_updated = cfg.get_parameters()["dayspan_updated"]
dayspan_created = cfg.get_parameters()["dayspan_created"]
endpoint = cfg.get_parameters()["endpoint"]
desired_activities_tmp = cfg.get_parameters()["desired_activities"]
desired_activities = [i.strip() for i in desired_activities_tmp.split(",")]
month_year_created = cfg.get_parameters()["month/year_created"]
month_year_updated = cfg.get_parameters()["month/year_updated"]

logging.info("Dayspan updated: %s" % dayspan_updated)
logging.info("Dayspan created: %s" % dayspan_created)
logging.info("Endpoint: %s" % endpoint)
logging.info("Desired activities: %s" % str(desired_activities))
logging.info("Month/Year updated: %s" % month_year_updated)
logging.info("Month/Year created: %s" % month_year_created)

# The colnames for Leads endpoint
fields_str_tmp = "company, billingCity, billingState, billingCountry, billingPostalCode,\
website, mainPhone, annualRevenue, numberOfEmployees, industry, sicCode, sfdcAccountId,\
externalCompanyId, id, mktoName, personType, mktoIsPartner, isLead, mktoIsCustomer,\
isAnonymous, salutation, firstName, middleName, lastName, email, phone, mobilePhone,\
fax, title, contactCompany, address, city, state, country, postalCode, personTimeZone,\
originalSourceType, originalSourceInfo, registrationSourceType, registrationSourceInfo,\
originalSearchEngine, originalSearchPhrase, originalReferrer, emailInvalid,\
emailInvalidCause, unsubscribed, unsubscribedReason, doNotCall, marketingSuspended,\
marketingSuspendedCause, blackListed, blackListedCause, mktoPersonNotes, sfdcType,\
sfdcContactId, anonymousIP, inferredCompany, inferredCountry, inferredCity,\
inferredStateRegion, inferredPostalCode, inferredMetropolitanArea,\
inferredPhoneAreaCode, emailSuspended, emailSuspendedCause, emailSuspendedAt, department, sfdcId,\
createdAt, updatedAt, cookies, externalSalesPersonId, leadPerson, leadRole, leadSource,\
leadStatus, leadScore, urgency, priority, relativeScore, relativeUrgency, rating, sfdcLeadId,\
sfdcLeadOwnerId, personPrimaryLeadInterest, leadPartitionId, leadRevenueCycleModelId,\
leadRevenueStageId, gender, facebookDisplayName, twitterDisplayName, linkedInDisplayName,\
facebookProfileURL, twitterProfileURL, linkedInProfileURL, facebookPhotoURL, twitterPhotoURL,\
linkedInPhotoURL, facebookReach, twitterReach, linkedInReach, facebookReferredVisits,\
twitterReferredVisits, linkedInReferredVisits, totalReferredVisits, facebookReferredEnrollments,\
twitterReferredEnrollments, linkedInReferredEnrollments, totalReferredEnrollments,\
lastReferredVisit, lastReferredEnrollment, syndicationId, facebookId, twitterId, linkedInId,\
acquisitionProgramId, mktoAcquisitionDate, mktoMarketingSuspendedReason, mktoEmailable,\
mktoIsEmployee, mktoExcludefromScoring, mktoExcludefromLifecycle, RecordTypeId,\
PartnerAccountId, EmailBouncedReason, EmailBouncedDate, Account_Name__c,\
C_Lead_Source_Original__c, Campaign__c, First_Name_Local__c, GSD_Region__c, Last_Name_Local__c,\
Lead_Id__c, Lead_Owner__c, Phone_Extension__c, SFDC_AccountID__c, SFDC_ContactID__c,\
AccountMatched__c, Adobe_Marketing_Cloud_ID__c, Adobe_Transaction_ID__c, Contact_Us_Comments__c, \
Demandbase_DUNS__c, Demandbase_ID__c, Demandbase_Industry__c, Demandbase_Size__c,\
Do_Not_Market_Until__c, Level__c, Marketing_Status__c, Product_Interest__c, Sub_Industry__c,\
Region__c, Cloud__c, Velocity_Business_Unit__c, Velocity__c, IsPartner, AccountSource, DunsNumber,\
NaicsCode, NaicsDesc, SicDesc, Location_ID__c, IsEmailBounced, Address_Line1__c, Left_Company__c,\
Contact_Lead_Status__c, GDPRoptIn, demographicScore, firmographicScore, socialScore,\
blacklistedDate, gDPROptInSource, campaignLatestDate, gDPRDoubleOptIn, gDPROptInDate,\
gDPRStrictOptIndirectmail, gDPRStrictOptInphone, gDPRStrictOptInSMS, wentSFDCDate,\
Marketo_Sync__c_contact, behaviorScore, functionalArea, annualRevenueRange,\
GDPRStrictOptIndataprocessing, GDPRAnalyticsInterestpc, GDPRApplicationsInterestpc,\
GDPRBusinessProcessServicesInterestpc, GDPRCloudInterestpc, GDPRSecurityInterestpc,\
GDPRWorkplaceandMobilityInterestpc, GDPRBankingCapitalInterestpc, GDPRCommMediaEntorTechInterestpc,\
GDPRConsumerRetailInterestpc, GDPREnergyInterestpc, GDPRHealthcareInterestpc,\
GDPRLifeSciencesInterestpc, GDPRInsuranceInterestpc, GDPRManufacturingInterestpc,\
GDPRPublicSectorInterestpc, GDPRTravelTransportationInterestpc, GDPRThriveNewsletterpc,\
GDPRBlogUpdatesInterestpc, topics, contactUsGroupRouting, uniqueDateTime, fastTrack, referring_url,\
DXCOfferingFamily, SLMSNewsletter, Velocity_Lead_Type__c, Preferred_Language__c,\
Reject_Reason_Marketing__c, Lead_Country__c, Sub_Region__c,\
Lead_Source_Campaign__c, Microsoft_Region__c, Business_Function__c, contactUsProcuctName,\
leadSourceMostRecent, contactUsRelationship, contactUsIndustrySpecification, contactUsSolutionArea,\
referringzoneslug, adobeVisitorID, gDPROptInPageSourced, lastWebPageVisited, areaofInterest,\
dUNSNumberDomesticUltimatemarketing"
fields_str = [i.strip() for i in fields_str_tmp.split(",")]

# Created filter
# Determine whether we want to get data from past X days or from a specific month/year.
if dayspan_created == '' and month_year_created != '':
    CREATED_DATE = True
    month = month_year_created[:3]
    year = int(month_year_created[4:])
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
    start_created = months[month][0][:10]
    end_created = months[month][1][:10]
elif dayspan_created != '' and month_year_created != '':
    CREATED_DATE = True
    logging.info('Disregarding the <Month/Year for \'Created\'> parameter, taking into consideration only the \
<How many days back you want to go with \'Created\'?> parameter')
    start_created = str((datetime.utcnow() - timedelta(days=int(dayspan_created)))
                        .date())
    end_created = str(datetime.utcnow().date())
elif dayspan_created == '' and month_year_created == '':
    CREATED_DATE = False
    logging.info('Created Date not provided')
elif dayspan_created != '' and month_year_created == '':
    CREATED_DATE = True
    start_created = str((datetime.utcnow() - timedelta(days=int(dayspan_created)))
                        .date())
    end_created = str(datetime.utcnow().date())

# Updated filter
# Determine whether we want to get data from past X days or from a specific month/year.
if dayspan_updated == '' and month_year_updated != '':
    UPDATED_DATE = True
    month = month_year_updated[:3]
    year = int(month_year_updated[4:])
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
    start_updated = months[month][0][:10]
    end_updated = months[month][1][:10]
elif dayspan_updated != '' and month_year_updated != '':
    UPDATED_DATE = True
    logging.info('Disregarding the <Month/Year for \'Updated\'> parameter, taking into consideration only the \
<How many days back you want to go with \'Updated\'?> parameter')
    start_updated = str((datetime.utcnow() - timedelta(days=int(dayspan_updated)))
                        .date())
    end_updated = str(datetime.utcnow().date())
elif dayspan_updated == '' and month_year_updated == '':
    UPDATED_DATE = False
    logging.info('Updated Date not provided')
elif dayspan_updated != '' and month_year_updated == '':
    UPDATED_DATE = True
    start_updated = str((datetime.utcnow() - timedelta(days=int(dayspan_updated)))
                        .date())
    end_updated = str(datetime.utcnow().date())

logging.info("params read")

# Destination to fetch and output files and tables
DEFAULT_TABLE_INPUT = "/data/in/tables/"
DEFAULT_FILE_INPUT = "/data/in/files/"

DEFAULT_FILE_DESTINATION = "/data/out/files/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"


def save_manifest(file_name, primary_keys):
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
            logging.info("Output manifest file ({0}) produced.".format(file))
    except Exception as e:
        logging.error("Could not produce output file manifest.")
        logging.error(e)

    return

# check response


def check_response(response, stage):
    if response.status_code != 200:
        print(stage + ' failed.')
        print('The response code is: ' + str(response.status_code))
        sys.exit(1)
    else:
        print(stage + ' passed.')


parameters_1 = {'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'client_credentials'}

# get the token
resp = requests.get(
    url='https://566-GCC-428.mktorest.com/identity/oauth/token', params=parameters_1)
check_response(resp, 'Obtaining access token')

access_token = resp.json()['access_token']

parameters_2 = {'access_token': access_token}

# endpoint Activities
if endpoint == 'Activities':
    body = {
        "format": "CSV"
    }
    if not CREATED_DATE:
        logging.info('The Activities endpoint requires Created Date interval!')
        sys.exit(1)
    else:
        body['filter'] = {}
        body['filter']['createdAt'] = {"startAt": start_created,
                                       "endAt": end_created}

    if not UPDATED_DATE:
        pass
    else:
        body['filter']['updatedAt'] = {"startAt": start_updated,
                                       "endAt": end_updated}
    if len(desired_activities) > 0:
        try:
            body['filter']['activityTypeIds'] = desired_activities
        except KeyError:
            body['filter'] = {}
            body['filter']['activityTypeIds'] = desired_activities
    else:
        pass

    # Create the export
    create_export = requests.post('https://566-GCC-428.mktorest.com/bulk/v1/activities/export/create.json',
                                  params=parameters_2, json=body)

    check_response(create_export, 'Creating export')
    if not create_export.json()['success']:
        logging.info('Creating export was not successfull.')
        logging.info('Errors:')
        logging.info(create_export.json()['errors'])

    export_id = create_export.json()['result'][0]['exportId']

    # Enqueue export
    enqueue_export = requests.post('https://566-GCC-428.mktorest.com/bulk/v1/activities/export/' +
                                   export_id + '/enqueue.json',
                                   params=parameters_2)

    check_response(enqueue_export, 'Enqueuing export')

    time.sleep(60)
    status_export = requests.get('https://566-GCC-428.mktorest.com/bulk/v1/activities/export/' +
                                 export_id + '/status.json',
                                 params=parameters_2)

    check_response(status_export, 'Getting status of the export')

    # Wait for them to prepare the export
    while status_export.json()['result'][0]['status'] != 'Completed':
        print('Export not ready, next check in 60 seconds.')
        time.sleep(60)
        status_export = requests.get('https://566-GCC-428.mktorest.com/bulk/v1/activities/export/' +
                                     export_id + '/status.json',
                                     params=parameters_2)
        check_response(status_export, 'Getting status of the export')

    # set up the name of the output file
    output_file = DEFAULT_TABLE_DESTINATION + endpoint + "_bulk.csv"

    # assemble the curl command and running it
    args = "curl \"https://566-GCC-428.mktorest.com/bulk/v1/activities/export/" + export_id + \
        "/file.json?access_token=" + access_token + "\"" + " > \"" + output_file + "\""
    subprocess.call(args, shell=True)

    # save the appropriate manifest
    file_name = endpoint + "_bulk.csv"
    save_manifest(file_name=file_name, primary_keys=['marketoGUID'])

# endpoint
elif endpoint == 'Leads':
    body = {
        "fields": fields_str,
        "format": "CSV"
    }

    if CREATED_DATE and UPDATED_DATE:
        body['filter'] = {}
        body['filter']['createdAt'] = {"startAt": start_created,
                                       "endAt": end_created}
        body['filter']['updatedAt'] = {"startAt": start_updated,
                                       "endAt": end_updated}
    elif CREATED_DATE and (not UPDATED_DATE):
        body['filter'] = {}
        body['filter']['createdAt'] = {"startAt": start_created,
                                       "endAt": end_created}
    elif (not CREATED_DATE) and UPDATED_DATE:
        body['filter'] = {}
        body['filter']['updatedAt'] = {"startAt": start_updated,
                                       "endAt": end_updated}
    elif CREATED_DATE or UPDATED_DATE:
        logging.info(
            'The Leads endpoint requires either Created or Updated parameter!')
        sys.exit(1)

    # Create the export
    create_export = requests.post('https://566-GCC-428.mktorest.com/bulk/v1/leads/export/create.json',
                                  params=parameters_2, json=body)

    check_response(create_export, 'Creating export')

    if not create_export.json()['success']:
        logging.info('Creating export was not successfull.')
        logging.info('Errors:')
        logging.info(create_export.json()['errors'])

    export_id = create_export.json()['result'][0]['exportId']

    enqueue_export = requests.post('https://566-GCC-428.mktorest.com/bulk/v1/leads/export/' +
                                   export_id + '/enqueue.json',
                                   params=parameters_2)

    check_response(enqueue_export, 'Enqueuing export')

    time.sleep(60)
    status_export = requests.get('https://566-GCC-428.mktorest.com/bulk/v1/leads/export/' +
                                 export_id + '/status.json',
                                 params=parameters_2)

    check_response(status_export, 'Getting status of the export')

    # Wait for them to prepare the export
    while status_export.json()['result'][0]['status'] != 'Completed':
        print('Export not ready, next check in 60 seconds.')
        time.sleep(60)
        status_export = requests.get('https://566-GCC-428.mktorest.com/bulk/v1/leads/export/' +
                                     export_id + '/status.json',
                                     params=parameters_2)
        check_response(status_export, 'Getting status of the export')

    output_file = DEFAULT_TABLE_DESTINATION + endpoint + "_bulk.csv"

    # assemble the curl command and run it
    args = "curl \"https://566-GCC-428.mktorest.com/bulk/v1/leads/export/" + export_id + \
        "/file.json?access_token=" + access_token + "\"" + " > \"" + output_file + "\""
    subprocess.call(args, shell=True)
    file_name = endpoint + "_bulk.csv"

    # save the manifest
    save_manifest(file_name=file_name, primary_keys=['id'])
else:
    logging.info('The endpoint is incorrectly specified.')
