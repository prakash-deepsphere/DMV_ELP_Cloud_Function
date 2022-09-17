import datetime
from google.cloud import bigquery

def Insert_Response_to_Bigquery(vAR_df):
    # vAR_df = vAR_df.astype({"ORDER_ID": int, "ORDER_GROUP_ID": int,"SG_ID":int})
    created_at = []
    created_by = []
    updated_at = []
    updated_by = []
    df_length = len(vAR_df)
    created_at += df_length * [datetime.datetime.utcnow()]
    created_by += df_length * ['AWS_LAMBDA_USER']
    updated_by += df_length * ['AWS_LAMBDA_USER']
    updated_at += df_length * [datetime.datetime.utcnow()]
    vAR_df['CREATED_DT'] = created_at
    vAR_df['CREATED_USER'] = created_by
    vAR_df['UPDATED_DT'] = updated_at
    vAR_df['UPDATED_USER'] = updated_by

    # Load client
    client = bigquery.Client(project='elp-2022-352222')

    # Define table name, in format dataset.table_name
    table = 'DMV_ELP.DMV_ELP_MLOPS_RESPONSE'
    job_config = bigquery.LoadJobConfig(autodetect=True,schema=[
# Specify the type of columns whose type cannot be auto-detected. For
# data type is ambiguous.
    bigquery.SchemaField("ORDER_GROUP_ID", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("ORDER_ID", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("SG_ID", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("CREATED_DT", bigquery.enums.SqlTypeNames.DATETIME,mode="REQUIRED"),
            bigquery.SchemaField("CREATED_USER", bigquery.enums.SqlTypeNames.STRING,mode="REQUIRED"),
            bigquery.SchemaField("UPDATED_DT", bigquery.enums.SqlTypeNames.DATETIME,mode="REQUIRED"),
            bigquery.SchemaField("UPDATED_USER", bigquery.enums.SqlTypeNames.STRING,mode="REQUIRED"),
            bigquery.SchemaField("REQUEST_DATE", bigquery.enums.SqlTypeNames.STRING,mode="REQUIRED"),
            bigquery.SchemaField("REQUEST_ID", bigquery.enums.SqlTypeNames.INT64,mode="REQUIRED"),
],write_disposition="WRITE_APPEND",)
        # Load data to BQ
    job = client.load_table_from_dataframe(vAR_df, table,job_config=job_config)

    job.result()  # Wait for the job to complete.
    table_id = 'elp-2022-352222.DMV_ELP.DMV_ELP_MLOPS_RESPONSE'
    table = client.get_table(table_id)  # Make an API request.
    print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )




def Insert_Response_to_Bigquery_Copy(vAR_df):
    # vAR_df = vAR_df.astype({"ORDER_ID": int, "ORDER_GROUP_ID": int,"SG_ID":int})
    created_at = []
    created_by = []
    updated_at = []
    updated_by = []
    df_length = len(vAR_df)
    created_at += df_length * [datetime.datetime.utcnow()]
    created_by += df_length * ['AWS_LAMBDA_USER']
    updated_by += df_length * ['AWS_LAMBDA_USER']
    updated_at += df_length * [datetime.datetime.utcnow()]
    vAR_df['CREATED_DT'] = created_at
    vAR_df['CREATED_USER'] = created_by
    vAR_df['UPDATED_DT'] = updated_at
    vAR_df['UPDATED_USER'] = updated_by

    # Load client
    client = bigquery.Client(project='elp-2022-352222')

    # Define table name, in format dataset.table_name
    table = 'DMV_ELP.DMV_ELP_MLOPS_RESPONSE_COPY'
    job_config = bigquery.LoadJobConfig(autodetect=True,schema=[
# Specify the type of columns whose type cannot be auto-detected. For
# data type is ambiguous.
    bigquery.SchemaField("ORDER_GROUP_ID", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("ORDER_ID", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("SG_ID", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("CREATED_DT", bigquery.enums.SqlTypeNames.DATETIME,mode="REQUIRED"),
            bigquery.SchemaField("CREATED_USER", bigquery.enums.SqlTypeNames.STRING,mode="REQUIRED"),
            bigquery.SchemaField("UPDATED_DT", bigquery.enums.SqlTypeNames.DATETIME,mode="REQUIRED"),
            bigquery.SchemaField("UPDATED_USER", bigquery.enums.SqlTypeNames.STRING,mode="REQUIRED"),
            bigquery.SchemaField("REQUEST_DATE", bigquery.enums.SqlTypeNames.STRING,mode="REQUIRED"),
            bigquery.SchemaField("REQUEST_ID", bigquery.enums.SqlTypeNames.INT64,mode="REQUIRED"),
],write_disposition="WRITE_APPEND",)
        # Load data to BQ
    job = client.load_table_from_dataframe(vAR_df, table,job_config=job_config)

    job.result()  # Wait for the job to complete.
    table_id = 'elp-2022-352222.DMV_ELP.DMV_ELP_MLOPS_RESPONSE_COPY'
    table = client.get_table(table_id)  # Make an API request.
    print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )