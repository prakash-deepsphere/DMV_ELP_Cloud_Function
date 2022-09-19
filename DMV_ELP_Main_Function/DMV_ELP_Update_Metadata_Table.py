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

def UpdateMetadataTable():
   vAR_num_of_updated_row =0
   vAR_client = bigquery.Client(project='elp-2022-352222')

   vAR_query_list = ["""UPDATE `DMV_ELP.DMV_ELP_REQUEST_RESPONSE_METADATA` SET RUN1 ='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1 is NULL AND RUN2 is NULL AND RUN3 is NULL AND
RUN4 is NULL AND RUN5 is NULL and date(CREATED_DT)=current_date()""",

"""UPDATE `DMV_ELP.DMV_ELP_REQUEST_RESPONSE_METADATA` SET RUN1 ='COMPLETE', RUN2='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1 ='IN PROGRESS' AND RUN2 is NULL AND RUN3 is NULL AND RUN4 is NULL AND RUN5 is NULL and date(CREATED_DT)=current_date()""",

"""UPDATE `DMV_ELP.DMV_ELP_REQUEST_RESPONSE_METADATA` SET RUN2 ='COMPLETE', RUN3='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER'  WHERE RUN1='COMPLETE' AND RUN2 ='IN PROGRESS' AND RUN3 is NULL AND RUN4 is NULL AND RUN5 is NULL and date(CREATED_DT)=current_date()""",

"""UPDATE `DMV_ELP.DMV_ELP_REQUEST_RESPONSE_METADATA` SET RUN3 ='COMPLETE', RUN4='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1='COMPLETE' AND RUN2 ='COMPLETE' AND RUN3 ='IN PROGRESS' AND RUN4 is NULL AND RUN5 is NULL and
date(CREATED_DT)=current_date()""",

"""UPDATE `DMV_ELP.DMV_ELP_REQUEST_RESPONSE_METADATA` SET RUN4 ='COMPLETE', RUN5='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE
RUN1 ='COMPLETE' AND RUN2 ='COMPLETE' AND RUN3 ='COMPLETE' AND RUN4= 'IN PROGRESS' AND RUN5 is NULL and date(CREATED_DT)=current_date()""",

"""UPDATE `DMV_ELP.DMV_ELP_REQUEST_RESPONSE_METADATA` SET RUN5 ='COMPLETE',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1 ='COMPLETE' 
AND RUN2 ='COMPLETE' AND RUN3 ='COMPLETE' AND RUN4 = 'COMPLETE' AND RUN5 ='IN PROGRESS'
and date(CREATED_DT)=current_date()"""]

   for query in vAR_query_list:

      vAR_job = vAR_client.query(query)
      vAR_job.result()
      vAR_num_of_updated_row = vAR_job.num_dml_affected_rows
      if vAR_num_of_updated_row==1:
         print('Metadata table update query executed - ',query)
         break
      else:
         print('Metadata table not updated')
