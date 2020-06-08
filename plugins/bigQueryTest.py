from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account

AUTH_JSON_PATH = "/mnt/c/plugins/Covid-2f95220d7160.json"
SCOPE_PATH = "https://www.googleapis.com/auth/cloud-platform"
DATASET_ID = "Aditi"

def load_data_into_bqtable(date):
    path = AUTH_JSON_PATH
    # creating credentials
    credentials = service_account.Credentials.from_service_account_file(
        path,
        scopes=[SCOPE_PATH]
    )
    # creating client
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id)
    dataset_id = DATASET_ID
    dataset_ref = client.dataset(dataset_id)
    # creating dataset if not exists
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))
    job_config = bigquery.LoadJobConfig()
    # autodetect schema from the given csv
    job_config.autodetect = True
    # only create if table not exists
    job_config.create_disposition = 'CREATE_IF_NEEDED'
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    url = f"airflow_home/plugins/Output/CovidStats({date}).csv"
    with open(url, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, dataset_ref.table("covid_data"), job_config=job_config
        )
    # API request
    print("Starting job {}".format(load_job.job_id))
    load_job.result()  # Waits for table load to complete.
    print("Job finished.")
    assert load_job.state == 'DONE'
    destination_table = client.get_table(dataset_ref.table("covid_data"))
    print("Loaded {} rows.".format(destination_table.num_rows))
    query = ('SELECT * FROM `{}.{}` WHERE date = "{}"'
             .format(dataset_id, 'covid_data', date))
    try:
        query_job = client.query(query)
        results = query_job.result()
        row_count = 0
        for row in results:
            row_count += 1

    except Exception as e:
        print("Error")
        print(e)

    return row_count


def load_status_into_bq_table():
    path = AUTH_JSON_PATH
    # creating credentials
    credentials = service_account.Credentials.from_service_account_file(
        path,
        scopes=[SCOPE_PATH]
    )
    # creating client
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id)
    dataset_id = DATASET_ID
    dataset_ref = client.dataset(dataset_id)
    # creating dataset if not exists
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))
    job_config = bigquery.LoadJobConfig()
    # autodetect schema from the given csv
    job_config.autodetect = True
    # only create if table not exists
    job_config.create_disposition = 'CREATE_IF_NEEDED'
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    url = f"airflow_home/plugins/Output/UploadPercentageStatus.csv"
    with open(url, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, dataset_ref.table("upload_status"), job_config=job_config
        )
    # API request
    print("Starting job {}".format(load_job.job_id))
    load_job.result()  # Waits for table load to complete.
    print("Job finished.")
    assert load_job.state == 'DONE'
    destination_table = client.get_table(dataset_ref.table("upload_status"))
    print("Loaded {} rows.".format(destination_table.num_rows))
