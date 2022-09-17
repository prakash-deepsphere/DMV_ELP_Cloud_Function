import pandas as pd
from google.cloud import bigquery



def Previously_Denied_Configuration_Validation(input_text):
    
    vAR_fword_data = Read_Previously_Denied_Configuration_Table()
    
    for vAR_index, vAR_row in vAR_fword_data.iterrows():
        if(input_text==vAR_row['PREVIOUSLY_DENIED_CONFIG']):
            return True,"Denied - "+vAR_row['DENIAL_REASON']
        else:
            return False,'Given configuration is not found in DMV Previously Denied Configuration list'
    
    
    
def Read_Previously_Denied_Configuration_Table():

    vAR_bqclient = bigquery.Client()

    vAR_query_string = """
    SELECT PREVIOUSLY_DENIED_CONFIG,DENIAL_REASON FROM `elp-2022-352222.DMV_ELP.DMV_ELP_PREVIOUSLY_DENIED_CONFIGURATION`
    """

    vAR_dataframe = (
        vAR_bqclient.query(vAR_query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    return vAR_dataframe