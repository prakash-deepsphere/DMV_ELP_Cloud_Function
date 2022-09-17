from google.cloud import bigquery

def DeleteProcessedConfigs():
   vAR_client = bigquery.Client(project='elp-2022-352222')

   vAR_query_delete = """
   delete from `elp-2022-352222.DMV_ELP.DMV_ELP_REQUEST` where CONFIGURATION in
(select Configuration from `elp-2022-352222.DMV_ELP.DMV_ELP_MLOPS_RESPONSE_COPY`  where date(created_dt) = current_date())
   """

   vAR_job = vAR_client.query(vAR_query_delete)
   vAR_job.result()
   print("Processed Records are deleted! - ",vAR_job.num_dml_affected_rows)