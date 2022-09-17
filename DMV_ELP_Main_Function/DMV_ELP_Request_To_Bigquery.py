

import datetime
from google.cloud import bigquery

def Insert_Request_To_Bigquery(vAR_batch_elp_configuration,vAR_number_of_configuration):

   vAR_config_df = vAR_batch_elp_configuration


   created_at = []
   created_by = []
   updated_at = []
   updated_by = []
   created_at += vAR_number_of_configuration * [datetime.datetime.utcnow()]
   created_by += vAR_number_of_configuration * ['AWS_LAMBDA_USER']
   updated_by += vAR_number_of_configuration * ['AWS_LAMBDA_USER']
   updated_at += vAR_number_of_configuration * [datetime.datetime.utcnow()]

   vAR_config_df['CONFIG_ID'] = range(1,vAR_number_of_configuration+1)
   vAR_config_df['CREATED_USER'] = created_by
   vAR_config_df['CREATED_DT'] = created_at
   vAR_config_df['UPDATED_USER'] = updated_by
   vAR_config_df['UPDATED_DT'] = updated_at

   client = bigquery.Client(project='elp-2022-352222')

   # Define table name, in format dataset.table_name
   table = 'DMV_ELP.DMV_ELP_REQUEST'
   job_config = bigquery.LoadJobConfig(autodetect=True,write_disposition="WRITE_APPEND",)
   job = client.load_table_from_dataframe(vAR_config_df, table,job_config=job_config)

   job.result()  # Wait for the job to complete.
   table_id = 'elp-2022-352222.DMV_ELP.DMV_ELP_REQUEST'
   table = client.get_table(table_id)  # Make an API request.
   print(
         "Loaded {} rows and {} columns to {}".format(
               table.num_rows, len(table.schema), table_id
         )
      )
   