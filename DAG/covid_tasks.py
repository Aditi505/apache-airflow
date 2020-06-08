#Importing modules
from datetime import timedelta, date
import urllib.request as request
import json
import pandas as pd
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from bigQueryTest import *


#Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

#Instantiate a DAG
dag = DAG(
    'covid_tasks', default_args=default_args,
    schedule_interval="@daily")

today = date.today().strftime("%Y-%d-%m")
globals()['saved_record_length'] = 0
globals()['uploaded_record_length'] = 0

#Tasks
def get_statewise_data():


    with request.urlopen('https://api.covid19india.org/data.json') as response:
        source = response.read()
        data = json.loads(source)

    statewise_dict = data['statewise']

    df = pd.DataFrame(statewise_dict, columns=['state', 'active'])
    date = []
    saved_record_length = len(df.index)
    for i in range(saved_record_length):
        date.append(today)
    df['date'] = date

    df.to_csv(f'/mnt/c/plugins/Output/CovidStats({today}).csv', index=False)
    saved_record_length_data = [[saved_record_length]]
    df2 = pd.DataFrame(saved_record_length_data, columns=["records"])
    df2.to_csv('/mnt/c/plugins/Output/saved_record_length.csv', index=False)


def upload_data():
    uploaded_record_length = BigQuery().load_data_into_bqtable(today)
    uploaded_record_length_data = [[uploaded_record_length]]
    df = pd.DataFrame(uploaded_record_length_data, columns=["records"])
    df.to_csv('/mnt/c/plugins/Output/uploaded_record_length.csv', index=False)

def upload_status():
    df = pd.read_csv('/mnt/c/plugins/Output/saved_record_length.csv')
    saved_record_length = df.at[0, 'records']

    df2 = pd.read_csv('/mnt/c/plugins/Output/uploaded_record_length.csv')
    uploaded_record_length = df2.at[0, 'records']

    percentage = float(uploaded_record_length) / float(saved_record_length) * 100
    data = [[today, percentage]]
    df = pd.DataFrame(data, columns=["Date", "Percentage of Upload"])
    df.to_csv('/mnt/c/plugins/Output/UploadPercentageStatus.csv', index=False)

    BigQuery().load_status_into_bq_table()


#Task1
get_data_operator = PythonOperator(task_id='get_covid_data_task', python_callable=get_statewise_data, dag=dag)

#Task2
upload_data_operator = PythonOperator(task_id='upload_covid_data_task', python_callable=upload_data, dag=dag)

#Task3
upload_status_operator = PythonOperator(task_id="upload_status_task", python_callable=upload_status, dag=dag)

#Setting up dependencies
get_data_operator >> upload_data_operator >> upload_status_operator
