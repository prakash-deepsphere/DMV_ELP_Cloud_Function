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

from google.cloud import bigquery


# Bigquery insert function needs to be implemented



def GetLastRequestId():
    vAR_last_request_id = 0
    vAR_client = bigquery.Client()
    vAR_query_job = vAR_client.query(
        """
       select REQUEST_ID from(
SELECT distinct REQUEST_ID,UPDATED_DT FROM `elp-2022-352222.DMV_ELP.DMV_ELP_MLOPS_RESPONSE` 
order by UPDATED_DT desc limit 1)"""
    )

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    print('Last Request Id - ',vAR_results)
    for row in vAR_results:
        vAR_last_request_id = row.get('REQUEST_ID')
    return vAR_last_request_id