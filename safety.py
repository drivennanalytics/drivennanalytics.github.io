# Use For Local Run
import sys
sys.setdefaultencoding('utf-8')
import os
sys.path.append("lib/")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "googlecred.json"
import numpy as np
import pickle
import sys
import pandas as pd
import requests
import warnings
import logging
import shutil
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

def get_google_credentials() :
    logging.info('trying to get google cred')
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('storage', 'v1', credentials=credentials)
    logging.info('successfully retrieved google credentials')

def savePandasToCloud(bucket,dataframe,filename) :
    get_google_credentials()
    dataframe.to_csv(filename)
    credentials = GoogleCredentials.get_application_default()
    storage = discovery.build('storage', 'v1', credentials=credentials)
    body = {'name': filename}
    req = storage.objects().insert(bucket=bucket, body=body, media_body=filename)
    resp = req.execute()
    return('Uploaded ' + str(filename) + ' to GCS. Bucket = ' + bucket)    


def get_raw_request_data() :
    #input api headers
    headers = {'Authorization': 'Token token="PazczhYmLJpDLN-_mLsV", email="tobias.schlottke@gmail.com"'}
    total_pages = requests.get("https://meditalente.de/api/ml/requests", headers=headers).json()['meta']['total_pages']
    request_data = [] #init empty list for dumping data
    for x in range(1, total_pages+1):
        status = 'Page Loaded: ' + str(x) + ' of ' + str(total_pages)
        logging.info(status)
        request_data.append(requests.get("https://meditalente.de/api/ml/requests?page=%s" % (x), headers=headers).json())
    return(request_data)

def get_raw_company_data() :
    #input api headers
    headers = {'Authorization': 'Token token="PazczhYmLJpDLN-_mLsV", email="tobias.schlottke@gmail.com"'}
    total_pages = requests.get("https://meditalente.de/api/ml/companies", headers=headers).json()['meta']['total_pages']
    company_data = [] #init empty list for dumping data
    for x in range(1, total_pages+1):
        status = 'Page Loaded: ' + str(x) + ' of ' + str(total_pages)
        logging.info(status)
        company_data.append(requests.get("https://meditalente.de/api/ml/companies?page=%s" % (x), headers=headers).json())
    return(company_data)

def get_raw_match_data() :
    #input api headers and initialize variables
    headers = {'Authorization': 'Token token="PazczhYmLJpDLN-_mLsV", email="tobias.schlottke@gmail.com"'}
    match_data = [] #init empty list for dumping data
    num_matches = 1 #initialize loop
    page_num = 0 #initialize 
    
    while num_matches > 0 :
        thisPage = requests.get("https://meditalente.de/api/ml/matches?page=%s" % (page_num), headers=headers).json()
        num_matches = len(thisPage['matches'])
        match_data.append(thisPage)
        status = 'Loaded ' + str(num_matches) + ' matches from page: ' + str(page_num)
        page_num += 1
        logging.info(status)
        print(status)
    return(match_data)

def extractRequestData():
    variables = ['id','lastname','phone','address','nationality','personal_status','current_company_id','year_of_birth','operational_area',
            'education','distance','status','minus_score','remind_me','gender','flexibility_score','sympathy_score','language_skills_score',
             'year_of_birth','qualification','computer_skills','driving_skills','salary','wanted_salary','minus_score',
            'education_score','location_type','match_probability','search_in_progress','request_text','additional_information']
    result = get_raw_request_data()
    rowNames = list(xrange(0,len(result),1))
    dataframe = pd.DataFrame(rowNames)
    dataframe.columns = ['row']
    for variable in variables:
        thisColumn = []
        for i in xrange(0,len(result)):
            try:
                this = pd.io.json.json_normalize(request_data[i]['requests'])[variable].values.tolist()
            except:
                this = 'None'
            thisColumn = thisColumn + this
        print('Completed Variable: %s',variable)
        thisColumn = pd.DataFrame(thisColumn)
        thisColumn.columns = [variable]
        dataframe = pd.concat([dataframe, thisColumn],axis=1,ignore_index=False)
    return(dataframe)

def extractCompanyData():
  company_data = get_raw_company_data()
  variables = ['id','name','category','subcategory','city','zipcode','number_of_patients',\
               'number_of_employees','overdue_matches_count','longitude','latitude','mails_clicked',\
               'mails_opened','operational_areas','email','priority_company','phone']
  dataframe = pd.io.json.json_normalize(company_data[0]['companies'])[variables]
  for i in xrange(1,len(company_data)):
    thisDF = pd.io.json.json_normalize(company_data[i]['companies'])[variables]
    dataframe = dataframe.append(thisDF)
    dataframe.rename(index=str,columns={"id":"company_ids"})
  return(dataframe)

def extractMatchData():
    matches = get_raw_match_data()
    df = pd.io.json.json_normalize(matches,'matches',record_prefix='matches_') #get a dataframe of all matches and metadata
    df = df[['matches_id','matches_request_id','matches_company_id','matches_details','matches_status',
    'matches_road_distance','matches_score','matches_job_interview_at']]
    return(df)


request_data = extractRequestData()
match_data = extractMatchData()
company_data = extractCompanyData()

# # # Write Clean Data To GCS
savePandasToCloud(bucket='request-data-test',dataframe = request_data,filename = 'employee_data_ml.csv')
savePandasToCloud(bucket='request-data-test',dataframe = match_data,filename = 'match_data_ml.csv')
savePandasToCloud(bucket='request-data-test',dataframe = company_data,filename = 'company_data_ml.csv')