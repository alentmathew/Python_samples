#!/usr/bin/env python
# https://developers.google.com/adwords/api/docs/samples/python/remarketing#upload-offline-data-for-store-sales-transactions 
#https://github.com/googleads/googleads-python-lib/blob/master/examples/adwords/v201809/remarketing/upload_offline_data.py

 # import pandas_gbq
 import pandas as pd
 import numpy as np
 import warnings
 import os
 import datetime
 # import hashlib
 from googleads import adwords
 import pytz
 import locale
 import sys
 import _locale
 
 _locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])
 
 _DT_FORMAT = '%Y%m%d %H%M%S'
 # The timezone to be used. For valid timezone IDs, see:
 # https://developers.google.com/adwords/api/docs/appendix/codes-formats  #timezone-ids
 _TIMEZONE = pytz.timezone('America/Toronto')
 
 # Store sales upload common metadata types
 METADATA_TYPE_1P = 'FirstPartyUploadMetadata'
 STORE_SALES_UPLOAD_COMMON_METADATA_TYPE = METADATA_TYPE_1P
 
 projectid = 'my-test-project'
 #the query provides data to be uploaded to google ads portal
 query = open(r'C:\Users\Alen\Downloads\sqlquery.sql', 'r').read()
 
 print('Starting conversion data uploads to event CONVERSION NAME')
 
 df = pd.read_gbq(query, project_id=projectid, dialect='standard')
 
 # The external upload ID can be any number that you use to keep track of your
 # uploads.
 EXTERNAL_UPLOAD_ID =  max(df['Date_Start_Day'].head(1).dt.strftime('%Y%m%d'))
 
 
 def _GetFormattedDateTime(dt):
     """Formats the given datetime and timezone for use with AdWords.
 
     Args:
     dt: a datetime instance.
 
     Returns:
     A str representation of the datetime in the correct format for AdWords.
     """
     return '%s %s' % (datetime.datetime.strftime(dt, _DT_FORMAT), _TIMEZONE.zone)
 
 # CONVERSION_NAME : Name of the conversion tracker to upload to.
 wholedata =[]
 for i in range(len(df)):
     CONVERSION_NAME = 'CONVERSION NAME'
     HASHED_EMAIL_VALUE = df.iloc[i, 0]
     CONVERSION_TIME = df.iloc[i, 1]
     COUNTRY_CODE_VALUE = 'IND'
     CONVERSION_CURRENCY = df.iloc[i, 2]
     CONVERSION_VALUE = df.iloc[i, 3]
 
     offline_data = {
         'StoreSalesTransaction': {
             'userIdentifiers': [{'userIdentifierType': 'HASHED_EMAIL', 'value': HASHED_EMAIL_VALUE},
                                 {'userIdentifierType': 'COUNTRY_CODE', 'value': COUNTRY_CODE_VALUE}],
             'transactionTime': _GetFormattedDateTime(datetime.datetime.strptime(CONVERSION_TIME, "%Y-%m-%d %H:%M:%S")),
             'transactionAmount': {
                 'currencyCode': CONVERSION_CURRENCY,
                 'money': {
                     'microAmount': (int(CONVERSION_VALUE * 1000000))
                 }
             },
             'conversionName': CONVERSION_NAME
         }
     }
 
     wholedata.append(offline_data)
 
 # divide the total conversions into lists of 50 elements
 
 subsetdata = [wholedata[i:i + 50] for i in range(0, len(wholedata), 50)]
 
 
 def _GetFieldPathElementIndex(api_error, field):
     """Retrieve the index of a given field in the api_error's fieldPathElements.
 
     Args:
     api_error: a dict containing a partialFailureError returned from the AdWords
       API.
     field: a str field for which this determines the index in the api_error's
       fieldPathElements.
 
     Returns:
     An int index of the field path element, or None if the specified field can't
     be found in the api_error.
     """
     field_path_elements = api_error['fieldPathElements']
 
     if field_path_elements:
         found_index = [field_path_element['index']
                        for field_path_element in field_path_elements
                        if field_path_element['field'] == field]
         if found_index:
             return found_index
 
     return None
 
 def data_uploads(external_upload_id,
          store_sales_upload_common_metadata_type
          ):
     # Set partial failure to True since this example demonstrates how to handle
     # partial errors. #https://github.com/googleads/googleads-python-lib/blob/master/googleads.yaml
     client = adwords.AdWordsClient.LoadFromStorage(r'C:\Users\Alen\Downloads\googleads.yaml')
     client.partial_failure = True
     # Initialize appropriate services.
     offline_data_upload_service = client.GetService(
         'OfflineDataUploadService', version='v201809')
 
 
     # Set the type and metadata of this upload.
     upload_metadata = {
         'StoreSalesUploadCommonMetadata': {
             'xsi_type': store_sales_upload_common_metadata_type,
             'loyaltyRate': 1.0,
             'transactionUploadRate': 1.0,
         }
     }
 
     if store_sales_upload_common_metadata_type == METADATA_TYPE_1P:
         upload_type = 'STORE_SALES_UPLOAD_FIRST_PARTY'
 
     else:
         raise ValueError('Unknown metadata type.')
 
     # Create offline data upload
     for i in subsetdata:
         offline_data_upload = {
             'externalUploadId': external_upload_id, # + str(subsetdata.index(i)),
             'offlineDataList': i,
             # Set the type of this upload.
             'uploadType': upload_type,
             'uploadMetadata': upload_metadata
         }
 
         # Create an offline data upload operation.
         operations = [{
             'operator': 'ADD',
             'operand': offline_data_upload
         }]
 
         # Upload offline data on the server and print(the result.)
         result = offline_data_upload_service.mutate(operations)
         offline_data_upload = result['value'][0]
 
         print('Uploaded offline data with external upload ID "%d" and upload status '
               '"%s".' % (offline_data_upload['externalUploadId'],
                          offline_data_upload['uploadStatus']))
 
         # Print any partial data errors from the response.
         if result['partialFailureErrors']:
             for api_error in result['partialFailureErrors']:
 
                 # Get the index of the failed operation from the error's field path
                 # elements.
                 operation_index = _GetFieldPathElementIndex(api_error, 'operations')
 
                 if operation_index:
                     failed_offline_data_upload = operations[operation_index]['operand']
                     # Get the index of the entry in the offline data list from the error's
                     # field path elements.
                     offline_data_list_index = _GetFieldPathElementIndex(
                         api_error, 'offlineDataList')
                     print('Offline data list entry "%d" in operation "%d" with external '
                         'upload ID "%d" and type "%s" has triggered failure for the '
                         'following reason: "%s"' % (
                             offline_data_list_index, operation_index,
                             failed_offline_data_upload['externalUploadId'],
                             failed_offline_data_upload['uploadType'],
                             api_error['errorString']))
                 else:
                     print('A failure has occurred for the following reason: "%s".' % (
                         api_error['errorString']))
 
 
 
 data_uploads(EXTERNAL_UPLOAD_ID, STORE_SALES_UPLOAD_COMMON_METADATA_TYPE)