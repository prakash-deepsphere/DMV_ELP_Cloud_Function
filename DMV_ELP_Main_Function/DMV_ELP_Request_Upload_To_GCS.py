from google.cloud import storage
import datetime
import os

def Upload_Request_GCS(vAR_request):
   
    vAR_request = vAR_request.to_csv()
    vAR_bucket_name = os.environ['GCS_BUCKET_NAME']
    vAR_utc_time = datetime.datetime.utcnow()
    client = storage.Client()
    bucket = client.get_bucket(vAR_bucket_name)
    vAR_file_path = 'requests/'+vAR_utc_time.strftime('%Y%m%d')+'/dmv_api_request_'+vAR_utc_time.strftime('%H%M%S')+'.csv'
    bucket.blob(vAR_file_path).upload_from_string(vAR_request, 'text/csv')
    print('ELP Configuration Request successfully saved into cloud storage')
    print('Path - ',vAR_file_path)
    return vAR_file_path