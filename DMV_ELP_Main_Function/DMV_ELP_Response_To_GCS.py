import datetime
from google.cloud import storage

def Upload_Response_GCS(vAR_result):
    
   vAR_bucket_name = 'dmv_elp_project'
   vAR_utc_time = datetime.datetime.utcnow()
   client = storage.Client()
   bucket = client.get_bucket(vAR_bucket_name)
   bucket.blob('response/dmv_api_result/'+vAR_utc_time.strftime('%Y%m%d')+'/'+vAR_utc_time.strftime('%H%M%S')+'.csv').upload_from_string(vAR_result, 'text/csv')
   print('API Response successfully saved into cloud storage')