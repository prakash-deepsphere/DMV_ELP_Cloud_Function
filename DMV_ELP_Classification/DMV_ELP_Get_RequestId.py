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