import pandas as pd
from google.cloud import bigquery



def FWord_Validation(input_text):
    
    vAR_fword_data = Read_FWord_Guideline_Table()
    
    for vAR_index, vAR_row in vAR_fword_data.iterrows():
        if(input_text==vAR_row['CONFIGURATION'] and vAR_row['APPROVED_OR_DENIED']=='Approved'):
            return False,vAR_row['APPROVED_OR_DENIED'] + " - "+vAR_row['REASON']
        elif(input_text==vAR_row['CONFIGURATION'] and vAR_row['APPROVED_OR_DENIED']=='Denied'):
            return True,vAR_row['APPROVED_OR_DENIED'] + " - "+vAR_row['REASON']
        else:
            return False,'Given configuration is not found in DMV FWords Guideline'
    
    
    
def Read_FWord_Guideline_Table():

    vAR_bqclient = bigquery.Client()

    vAR_query_string = """
    SELECT CONFIGURATION,REASON,APPROVED_OR_DENIED FROM `elp-2022-352222.DMV_ELP.DMV_ELP_CONFIGURATION_GUIDELINES`
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