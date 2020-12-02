from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'Dan-Udacity-Data-Engineering-Nanodegree',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG("airflow-udacity",
          default_args=default_args,
          description='Extract, Load, Transform data from S3 to Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_Execution',  dag=dag)

immigration_to_redshift = StageToRedshiftOperator(
    task_id='Immigration_Fact_Table',
    dag=dag,
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_bucket = 'udacity-dend-capstone',
    s3_prefix = 'immigration.parquet',
    schema = 'public',
    table = 'immigration',
)

state_to_redshift = StageToRedshiftOperator(
    task_id='State_Dimension_Table',
    dag=dag,
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_bucket = 'udacity-dend-capstone',
    s3_prefix = 'state.parquet',
    schema = 'public',
    table = 'state',
)

date_to_redshift = StageToRedshiftOperator(
    task_id='Date_Dimension_Table',
    dag=dag,
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_bucket = 'udacity-dend-capstone',
    s3_prefix = 'date.parquet',
    schema = 'public',
    table = 'date',
)

run_quality_checks = DataQualityOperator(
    task_id='Data_Quality_Checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables=['immigration', 'state', 'date'],
)

end_operator = DummyOperator(task_id='End_Execution',  dag=dag)

start_operator >> immigration_to_redshift >> [state_to_redshift, date_to_redshift]
[state_to_redshift, date_to_redshift] >> run_quality_checks >> end_operator
