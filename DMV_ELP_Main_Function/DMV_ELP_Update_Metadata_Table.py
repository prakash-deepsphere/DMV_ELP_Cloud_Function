from google.cloud import bigquery

def UpdateMetadataTable():
   vAR_num_of_updated_row =0
   vAR_client = bigquery.Client(project='elp-2022-352222')

   vAR_query_list = ["""UPDATE `DMV_ELP.DMV_ELP_REQUEST_RESPONSE_METADATA` SET SCHEDULE1 ='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE Schedule1 is NULL AND Schedule2 is NULL AND Schedule3 is NULL AND
Schedule4 is NULL AND Schedule5 is NULL and date(CREATED_DT)=current_date()""",

"""UPDATE `DMV_ELP.DMV_ELP_REQUEST_RESPONSE_METADATA` SET SCHEDULE1 ='COMPLETE', SCHEDULE2='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE Schedule1 ='IN PROGRESS' AND Schedule2 is NULL AND Schedule3 is NULL AND Schedule4 is NULL AND Schedule5 is NULL and date(CREATED_DT)=current_date()""",

"""UPDATE `DMV_ELP.DMV_ELP_REQUEST_RESPONSE_METADATA` SET SCHEDULE2 ='COMPLETE', SCHEDULE3='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER'  WHERE Schedule1='COMPLETE' AND Schedule2 ='IN PROGRESS' AND Schedule3 is NULL AND Schedule4 is NULL AND Schedule5 is NULL and date(CREATED_DT)=current_date()""",

"""UPDATE `DMV_ELP.DMV_ELP_REQUEST_RESPONSE_METADATA` SET SCHEDULE3 ='COMPLETE', SCHEDULE4='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE Schedule1='COMPLETE' AND Schedule2 ='COMPLETE' AND Schedule3 ='IN PROGRESS' AND Schedule4 is NULL AND Schedule5 is NULL and
date(CREATED_DT)=current_date()""",

"""UPDATE `DMV_ELP.DMV_ELP_REQUEST_RESPONSE_METADATA` SET SCHEDULE4 ='COMPLETE', SCHEDULE5='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE
Schedule1 ='COMPLETE' AND Schedule2 ='COMPLETE' AND Schedule3 ='COMPLETE' AND Schedule4= 'IN PROGRESS' AND Schedule5 is NULL and date(CREATED_DT)=current_date()""",

"""UPDATE `DMV_ELP.DMV_ELP_REQUEST_RESPONSE_METADATA` SET SCHEDULE5 ='COMPLETE',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE Schedule1 ='COMPLETE' 
AND Schedule2 ='COMPLETE' AND Schedule3 ='COMPLETE' AND Schedule4 = 'COMPLETE' AND Schedule5 ='IN PROGRESS'
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
