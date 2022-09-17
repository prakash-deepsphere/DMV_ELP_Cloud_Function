import boto3
from io import StringIO
import datetime
import os

def Upload_Response_To_S3(vAR_result):
    
   vAR_bucket_name = os.environ['S3_BUCKET_NAME']
   vAR_csv_buffer = StringIO()
   vAR_result.to_csv(vAR_csv_buffer)
   vAR_s3_resource = boto3.resource('s3',aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
   vAR_utc_time = datetime.datetime.utcnow()
   vAR_s3_resource.Object(vAR_bucket_name, 'batch/simpligov/new/ELP_Project_Response/'+vAR_utc_time.strftime('%Y%m%d')+'/ELP_Response'+'_'+vAR_utc_time.strftime('%H%M%S')+'.csv').put(Body=vAR_csv_buffer.getvalue())
   print('API Response successfully saved into S3 bucket')