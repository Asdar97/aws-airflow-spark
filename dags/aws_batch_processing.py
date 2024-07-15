import os
from datetime import datetime, timedelta
import logging
import json
from dotenv import load_dotenv, find_dotenv

from airflow import models
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

from plugins.scripts.data_generator import generate_ewallet_transactions

load_dotenv(find_dotenv())

BUCKET_NAME = "asdar-aws-batch"
REGION = "ap-southeast-1" #os.getenv("REGION")
AWS_CONN_ID = "aws_default"
FILE_PATH = "data/ewallet_transactions.csv"
LOCAL_SCRIPT = "plugins/scripts/ewallet_analysis.py"
S3_SCRIPT = "scripts/ewallet_analysis.py"
START_DATE = '2024-01-01'
JOB_FLOW_OVERRIDES_PATH = "dags/dict/job_flow_overrides.json"
SPARK_STEPS_PATH = "dags/dict/spark_steps.json"
S3_OUTPUT = "analysis/"

with open(JOB_FLOW_OVERRIDES_PATH, 'r') as file:
    JOB_FLOW_OVERRIDES = json.load(file)

with open(SPARK_STEPS_PATH, 'r') as file:
    SPARK_STEPS = json.load(file)

with models.DAG(
    "aws_batch_processing",
    schedule_interval='@once', 
    default_args={
        'owner': 'asdar',
        # 'email': ['mohdasdar97@gmail.com'],
        # 'email_on_failure': True,
        # 'retries': 1,
        # 'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aws', 'batch_processing', 'aws_emr', 'aws_glue', 'aws_athena'],
    max_active_tasks=5,
) as dag:
    
    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_ewallet_transactions,
        provide_context=True,
        op_kwargs={
            'file_path':FILE_PATH,      
        }
    )

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        region_name=REGION,
        aws_conn_id=AWS_CONN_ID
    )

    data_to_s3 = LocalFilesystemToS3Operator(
        task_id="data_to_s3",
        filename=FILE_PATH,
        dest_bucket=BUCKET_NAME,
        dest_key=FILE_PATH,
        replace=True,
        aws_conn_id=AWS_CONN_ID
    )

    script_to_s3 = LocalFilesystemToS3Operator(
        task_id="script_to_s3",
        filename=LOCAL_SCRIPT,
        dest_bucket=BUCKET_NAME,
        dest_key=S3_SCRIPT,
        replace=True,
        aws_conn_id=AWS_CONN_ID
    )

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        # emr_conn_id="emr_default",
        region_name=REGION,
        aws_conn_id=AWS_CONN_ID
    )

    define_emr_steps = EmrAddStepsOperator(
        task_id="define_emr_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS,
        params={
            "BUCKET_NAME": BUCKET_NAME,
            "S3_DATA": FILE_PATH,
            "S3_SCRIPT": S3_SCRIPT,
            "S3_OUTPUT": S3_OUTPUT
        },
        aws_conn_id=AWS_CONN_ID
    )

    last_step = len(SPARK_STEPS) - 1
    # wait for the steps to complete
    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='define_emr_steps', key='return_value')["
        + str(last_step)
        + "] }}",
        aws_conn_id=AWS_CONN_ID
    )

    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID
    )

    generate_data >> create_bucket >> [data_to_s3, script_to_s3] >> create_emr_cluster
    create_emr_cluster >> define_emr_steps >> step_checker >> terminate_emr_cluster