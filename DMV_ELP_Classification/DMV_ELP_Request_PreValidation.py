from google.cloud import bigquery
import pandas as pd
import datetime

def Pre_Request_Validation(request_json):
    vAR_error_message = ""

    if 'CONFIGURATION' not in request_json or len(request_json['CONFIGURATION'])==0 or request_json['CONFIGURATION']=='nan' or request_json['CONFIGURATION']=='<NA>':
        vAR_error_message =vAR_error_message+ "### Mandatory Parameter CONFIGURATION is missing"
    if 'SG_ID' not in request_json or len(request_json["SG_ID"])==0 or request_json['SG_ID']=='nan' or request_json['SG_ID']=='<NA>':
        vAR_error_message =vAR_error_message+ "### Mandatory Parameter Simply Gov Id is missing"
    if 'ORDER_GROUP_ID' not in request_json or len(request_json["ORDER_GROUP_ID"])==0 or request_json['ORDER_GROUP_ID']=='nan' or request_json['ORDER_GROUP_ID']=='<NA>':
        vAR_error_message =vAR_error_message+ "### Mandatory Parameter ORDER_GROUP_ID is missing"
    if 'ORDER_ID' not in request_json or len(request_json["ORDER_ID"])==0 or request_json['ORDER_ID']=='nan' or request_json['ORDER_ID']=='<NA>':
        vAR_error_message = vAR_error_message+"### Mandatory Parameter ORDER_ID is missing"
    if 'ORDER_DATE' not in request_json or len(request_json["ORDER_DATE"])==0 or request_json['ORDER_DATE']=='nan' or request_json['ORDER_DATE']=='<NA>':
        vAR_error_message = vAR_error_message+"### Mandatory Parameter ORDER_DATE is missing"

    if len(request_json['CONFIGURATION'])>7:
        vAR_error_message = vAR_error_message+"### ELP Configuration can not be more than 7 characters"

    if len(CheckIfConfigAlreadyProcessed(request_json['CONFIGURATION']))>0:
        vAR_error_message = vAR_error_message+"### Configuration skipped(Already processed)"
    return {"Error Message":vAR_error_message}


def CheckIfConfigAlreadyProcessed(vAR_config):
    vAR_client = bigquery.Client()
    vAR_processed_config = ''
    vAR_query = "select CONFIGURATION from `elp-2022-352222.DMV_ELP.DMV_ELP_MLOPS_RESPONSE_COPY` where CONFIGURATION='"+vAR_config+"'"+" group by CONFIGURATION having count(CONFIGURATION)>1"
    vAR_query_job = vAR_client.query(vAR_query)

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    for row in vAR_results:
        vAR_processed_config = row.get('CONFIGURATION')
    print('vAR_processed_config_query - ',vAR_query)
    print('vAR_processed_config - ',vAR_processed_config)
    return vAR_processed_config