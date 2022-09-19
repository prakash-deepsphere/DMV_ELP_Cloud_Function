"""
© Copyright 2022, California, Department of Motor Vehicle, all rights reserved.
The source code and all its associated artifacts belong to the California Department of Motor Vehicle (CA, DMV), and no one has any ownership
and control over this source code and its belongings. Any attempt to copy the source code or repurpose the source code and lead to criminal
prosecution. Don't hesitate to contact DMV for further information on this copyright statement.

Release Notes and Development Platform:
The source code was developed on the Google Cloud platform using Google Cloud Functions serverless computing architecture. The Cloud
Functions gen 2 version automatically deploys the cloud function on Google Cloud Run as a service under the same name as the Cloud
Functions. The initial version of this code was created to quickly demonstrate the role of MLOps in the ELP process and to create an MVP. Later,
this code will be optimized, and Python OOP concepts will be introduced to increase the code reusability and efficiency.
____________________________________________________________________________________________________________
Development Platform                | Developer       | Reviewer   | Release  | Version  | Date
____________________________________|_________________|____________|__________|__________|__________________
Google Cloud Serverless Computing   | DMV Consultant  | Ajay Gupta | Initial  | 1.0      | 09/18/2022

"""

import pandas as pd
from google.cloud import bigquery



def FWord_Validation(input_text):
    
    vAR_fword_data = Read_FWord_Guideline_Table()
    
    for vAR_index, vAR_row in vAR_fword_data.iterrows():
        if(input_text==vAR_row['CONFIGURATION'] and vAR_row['APPROVED_OR_DENIED']=='Approved'):
            return False,vAR_row['APPROVED_OR_DENIED'] + " - "+vAR_row['REASON']
        elif(input_text==vAR_row['CONFIGURATION'] and vAR_row['APPROVED_OR_DENIED']=='Denied'):
            return True,vAR_row['APPROVED_OR_DENIED'] + " - "+vAR_row['REASON']
        else:
            return False,'Given configuration is not found in DMV FWords Guideline'
    
    
    
def Read_FWord_Guideline_Table():

    vAR_bqclient = bigquery.Client()

    vAR_query_string = """
    SELECT CONFIGURATION,REASON,APPROVED_OR_DENIED FROM `elp-2022-352222.DMV_ELP.DMV_ELP_CONFIGURATION_GUIDELINES`
    """

    vAR_dataframe = (
        vAR_bqclient.query(vAR_query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    return vAR_dataframe