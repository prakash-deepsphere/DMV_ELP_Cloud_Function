from google.cloud import bigquery
import pandas as pd
import datetime

def GetLastErrorId():
    vAR_last_error_id = 0
    vAR_client = bigquery.Client()
    vAR_query_job = vAR_client.query(
        """
       select max(ERROR_ID) as max_error_id from `elp-2022-352222.DMV_ELP.DMV_ELP_ERROR_LOG`
"""
    )

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    print('Last Error Id - ',vAR_results)
    for row in vAR_results:
        vAR_last_error_id = row.get('max_error_id')
    return vAR_last_error_id



def InsertErrorLog(vAR_response_dict):
   del vAR_response_dict['Process Time']
   vAR_result = {}
   created_at = []
   created_by = []
   updated_at = []
   updated_by = []
   created_at = 1 * [datetime.datetime.utcnow()]
   created_by = 1 * ['AWS_LAMBDA_USER']
   updated_by = 1 * ['AWS_LAMBDA_USER']
   updated_at = 1 * [datetime.datetime.utcnow()]
   
   if GetLastErrorId() is not None:
      vAR_result['ERROR_ID'] = GetLastErrorId()+1
   else:
      vAR_result['ERROR_ID'] = 1
   vAR_result['ERROR_CODE'] = vAR_response_dict['ERROR_MESSAGE']
   vAR_result['ERROR_CONTEXT'] = str(vAR_response_dict)
   vAR_result['ERROR_MESSAGE'] = vAR_response_dict['ERROR_MESSAGE']
   vAR_result['CONFIGURATION'] = vAR_response_dict['CONFIGURATION']

   vAR_df = pd.DataFrame(vAR_result,index=[0])
   vAR_df['CREATED_DT'] = created_at
   vAR_df['CREATED_USER'] = created_by
   vAR_df['UPDATED_DT'] = updated_at
   vAR_df['UPDATED_USER'] = updated_by

   # Load client
   client = bigquery.Client(project='elp-2022-352222')

   # Define table name, in format dataset.table_name
   table = 'DMV_ELP.DMV_ELP_ERROR_LOG'
   job_config = bigquery.LoadJobConfig(autodetect=True,write_disposition='WRITE_APPEND')
   job = client.load_table_from_dataframe(vAR_df, table,job_config=job_config)

   job.result()  # Wait for the job to complete.
   table_id = 'elp-2022-352222.DMV_ELP.DMV_ELP_ERROR_LOG'
   table = client.get_table(table_id)  # Make an API request.
   print(
            "Error Log table Loaded {} rows and {} columns to {}".format(
               table.num_rows, len(table.schema), table_id
            )
      )
   