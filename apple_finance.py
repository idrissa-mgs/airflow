import json
import urllib
from datetime import datetime
from datetime import timedelta
import pandas
from urllib.request import urlopen
from io import StringIO, BytesIO


import logging
import boto3
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


s3_hook = S3Hook(aws_conn_id="my_conn_S3", region_name="eu-central-1")
bucket_name = "apple-data-airflow"
region = "eu-central-1"
api_key = "79b6e05dbc6900b3b6e19680b5eff78f"

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    #'start_date': datetime.now(),
    #'email': ['dieguimagassa10@gmail.com'],
    #'email_on_failure': True,
    #'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=0.5),
}
dag = DAG(
    dag_id='apple_finance',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['apple-finance'],
)


def get_apple_data():
    
    #get quotes

    quotes_url = f"https://financialmodelingprep.com/api/v3/profile/AAPL?apikey={api_key}"
    response = urlopen(quotes_url)
    quotes_data_ = response.read().decode("utf-8")
    quotes_data = json.loads(quotes_data_)
    pro_data= dict({key: val for key ,val in quotes_data[0].items() if key in ['companyName','price']})

    #gets_ratings
    rates_url = f"https://financialmodelingprep.com/api/v3/rating/AAPL?apikey={api_key}"
    response_r = urlopen(rates_url)
    rates_data_ = response_r.read().decode("utf-8")
    rates_data = json.loads(rates_data_)[0]

    rates_data = dict({key:val for key, val in rates_data.items() if key in ['rating','ratingScore', 'ratingRecommendation']}) 
    
    pro_data.update(rates_data)

    #df = pandas.DataFrame(pro_data , index = [0])
    pro_data['timestamp'] = datetime.now().isoformat()
    return pro_data


t1 = PythonOperator(
    task_id="get-APPL-data",
    python_callable = get_apple_data,
     provide_context=True,
    dag=dag
)

def create_bucket():
    
    
    logger = logging.getLogger("airflow.task")
    try:


        s3_hook.create_bucket(bucket_name = bucket_name, region_name= region)

        #message = 
        logger.info("--------BUCKET  CREATED---------")
        
    except ClientError as e:

        logger.error(e)
        logger.info("----------Can't CREATE BUCKET------------")
        return False
    return True


t2 = PythonOperator(
    task_id="create-s3-bucket",
    python_callable=create_bucket,
    dag=dag
)

def sendDataToS3(**kwargs):

    ti = kwargs['ti']
    

    apple_data = ti.xcom_pull(task_ids="get-APPL-data")
    apple_df = pandas.DataFrame(apple_data , index = [0])
    csv_buffer = StringIO()
    apple_df.to_csv(csv_buffer)

    s3_hook._upload_file_obj(file_obj = csv_buffer, key = f"apple_data_{datetime.now()}.csv",
    bucket_name = bucket_name)
    


t3 = PythonOperator(
    task_id="UploadToS3",
    python_callable=sendDataToS3,
     provide_context=True,
    dag=dag,
   
    
)

t1 >> t2 >> t3
