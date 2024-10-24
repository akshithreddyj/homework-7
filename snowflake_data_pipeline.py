from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 18),
    'retries': 1,
}

data_pipeline_dag = DAG(
    'snowflake_data_pipeline',
    default_args=default_args,
    description='Pipeline to create tables and load data into Snowflake from S3',
    schedule_interval='@daily',
    catchup=False
)

# Combined task for table creation
create_snowflake_tables = SnowflakeOperator(
    task_id='initialize_snowflake_tables',
    sql="""
    CREATE TABLE IF NOT EXISTS dev.raw_data.user_activity_log (
        user_id INT NOT NULL,
        session_id VARCHAR(32) PRIMARY KEY,
        activity_channel VARCHAR(32) DEFAULT 'direct'
    );
    CREATE TABLE IF NOT EXISTS dev.raw_data.session_logs (
        session_id VARCHAR(32) PRIMARY KEY,
        event_timestamp TIMESTAMP
    );
    """,
    snowflake_conn_id='snowflake_conn',
    dag=data_pipeline_dag
)

# Stage creation task
create_snowflake_stage = SnowflakeOperator(
    task_id='create_s3_stage',
    sql="""
    CREATE OR REPLACE STAGE dev.raw_data.s3_stage
    URL = 's3://s3-geospatial/readonly/'
    FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
    """,
    snowflake_conn_id='snowflake_conn',
    dag=data_pipeline_dag
)

# Combined task for data loading
load_data_into_snowflake = SnowflakeOperator(
    task_id='load_data_from_s3_to_snowflake',
    sql="""
    COPY INTO dev.raw_data.user_activity_log
    FROM @dev.raw_data.s3_stage/user_activity_log.csv;
    
    COPY INTO dev.raw_data.session_logs
    FROM @dev.raw_data.s3_stage/session_logs.csv;
    """,
    snowflake_conn_id='snowflake_conn',
    dag=data_pipeline_dag
)

# Define task dependencies
create_snowflake_tables >> create_snowflake_stage >> load_data_into_snowflake
