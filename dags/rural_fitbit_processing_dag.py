from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

ACTIVITY_TYPES = ['heart', 'calories', 'steps', 'elevation', 'distance', 'floors']

dag = DAG(
    'rural_fitbit_processing',
    default_args=default_args,
    description='Process Rural Fitbit data from S3',
    schedule_interval='0 0 * * 0',  # Weekly on Sunday at midnight
    catchup=False
)

# Sensor to check for new zip files
check_s3_files = S3KeySensor(
    task_id='check_s3_files',
    bucket_key='Onboarding/RADx/RURAL/fitbit_intraday/*.zip',
    bucket_name='umms-research-radx-image-processing',
    aws_conn_id='aws_default',
    timeout=60 * 60,  # 1 hour timeout
    poke_interval=60,  # Check every minute
    dag=dag
)

# Create parallel activity processing tasks
with TaskGroup(group_id='process_activities', dag=dag) as activity_group:
    for activity in ACTIVITY_TYPES:
        GlueJobOperator(
            task_id=f'process_{activity}',
            job_name=f'rural_fitbit_{activity}_processor',
            script_location='s3://your-glue-scripts-bucket/rural_fitbit_processor.py',
            job_parameters={
                '--activity_type': activity,
                '--source_bucket': 'umms-research-radx-image-processing',
                '--source_prefix': 'Onboarding/RADx/RURAL/fitbit_intraday',
                '--dest_bucket': 'umasschan-plum-data-lab',
                '--dest_prefix': 'Rural/intraday'
            },
            aws_conn_id='aws_default',
            region_name='your-region',
            dag=dag
        )

# Load to staging tables
load_staging = GlueJobOperator(
    task_id='load_staging_tables',
    job_name='rural_fitbit_staging_loader',
    script_location='s3://your-glue-scripts-bucket/load_staging.py',
    aws_conn_id='aws_default',
    region_name='your-region',
    dag=dag
)

# Merge to production
merge_production = GlueJobOperator(
    task_id='merge_production_tables',
    job_name='rural_fitbit_production_merger',
    script_location='s3://your-glue-scripts-bucket/merge_production.py',
    aws_conn_id='aws_default',
    region_name='your-region',
    dag=dag
)

# Set up dependencies
check_s3_files >> activity_group >> load_staging >> merge_production 