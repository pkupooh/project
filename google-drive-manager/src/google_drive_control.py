#!/usr/bin/env python
# coding: utf-8

# In[83]:


import pickle
import os
import pandas_gbq
import pandas as pd
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request
from datetime import datetime
from googleapiclient import discovery
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# create folder in google drive test

dev_key_path = "/Users/gowid/Desktop/master/transaction_analysis/config/gowid-gcs-dev_keyfile.json"
prd_key_path = "/Users/gowid/Desktop/master/transaction_analysis/config/gowid-gcs-prd-f069a99a7d10.json"

# sql_path = '/Users/gowid/Desktop/master/cashflow_analysis/sql/'

dev_credentials = service_account.Credentials.from_service_account_file(dev_key_path,scopes=['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/bigquery','https://www.googleapis.com/auth/devstorage.full_control'])
prd_credentials = service_account.Credentials.from_service_account_file(prd_key_path,scopes=['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/bigquery','https://www.googleapis.com/auth/devstorage.full_control'])

#client 설정
client = storage.Client(credentials = dev_credentials, project = dev_credentials.project_id)
bq_client = bigquery.Client(credentials = dev_credentials, project = dev_credentials.project_id)

# 대상 법인 folder list
sql = f"""
SELECT cast(idxCorp as string) as idxCorp
     , appliedAt
     , issuanceStatus
FROM `gowid-gcs-prd.raw_stream_gowid.CardIssuanceInfo`
WHERE issuanceStatus in ('APPLY','ISSUED')
      AND cardType in ('GOWID')
      AND PARSE_DATE('%Y%m%d',FORMAT_DATETIME('%Y%m%d',appliedAt)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
ORDER BY idxCorp
"""

# 데이터 조회 쿼리 실행 결과
query_job = bq_client.query(sql)
df = query_job.to_dataframe()

# list create
corp_list = df['idxCorp'].to_list()
folder_id_list = []
url_list = []
ref_date = []

# create folder function
def create_folder(corp_list):
    google_drive_id = '1Jn0Dg0fSuE_pSKiKTQF6ontnAVinjnzK' # parent folder id
    dev_credentials = service_account.Credentials.from_service_account_file(dev_key_path,scopes=['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/bigquery','https://www.googleapis.com/auth/devstorage.full_control','https://www.googleapis.com/auth/cloud-platform'])
    service = discovery.build('drive', 'v3', credentials=dev_credentials)
    try:
        for name_list in corp_list:
            file_metadata = {
                    'name': name_list,
                    'mimeType': 'application/vnd.google-apps.folder'
                    ,'parents' : [google_drive_id]
                }
            file = service.files().create(supportsAllDrives=True, body=file_metadata, fields = "id").execute()
            file_id = file.get("id")
            request_body = {
                'role': 'reader',
                'type': 'anyone'
            }
            response_permission = service.permissions().create(fileId=file_id, body=request_body).execute()

            # Print Sharing URL
            response_share_link = service.files().get(fileId=file_id,fields='webViewLink').execute()
            url = list(response_share_link.values())[0]

            # Remove Sharing Permission
            service.permissions().delete(
                fileId=file_id,
                permissionId='anyoneWithLink'
            ).execute()

            print('Folder id :' + file_id + 'URL : ' + url)

            # append to list
            folder_id_list.append(file_id)
            url_list.append(url)

        return file.get('id')
    except HttpError as error:
        print(F'An error occurred: {error}')
        return None        

#execute function
create_folder(corp_list)


# list to dataframe
drive_table = pd.DataFrame(list(zip(corp_list, folder_id_list, url_list)),columns = ['corp_id','folder_id','drive_url'])
ref_date = datetime.today().strftime('%Y-%m-%d')
drive_table['ref_date'] = ref_date


# dataframe to Bigquery
credentials = service_account.Credentials.from_service_account_file(
dev_key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

project_id = 'gowid-gcs-dev'

bq_client=bigquery.Client(project='gowid-gcs-dev').from_service_account_json(
            json_credentials_path=dev_key_path)

# Construct a BigQuery client object.
table_id = 'gowid-gcs-dev.gowid_credit_report.crm_document_drive'

drive_table.to_gbq(table_id,project_id ,if_exists='append', credentials = credentials) 

