from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

data_processing_dag = DAG(
    'snowflake_data_analytics_pipeline',
    default_args=default_args,
    description='Pipeline to create and process analytics tables from raw data in Snowflake',
    schedule_interval='@daily',
    catchup=False
)

create_schema_task = SnowflakeOperator(
    task_id='initialize_analytics_schema',
    sql="CREATE SCHEMA IF NOT EXISTS dev.analytics_data;",
    snowflake_conn_id='snowflake_conn',
    dag=data_processing_dag
)

generate_session_summary = SnowflakeOperator(
    task_id='generate_session_summary_table',
    sql="""
    CREATE OR REPLACE TABLE dev.analytics_data.session_summary AS
    WITH cleaned_sessions AS (
        SELECT 
            sessionId,
            ts,
            ROW_NUMBER() OVER (PARTITION BY sessionId ORDER BY ts) as row_num
        FROM dev.raw_data.session_timestamp
        WHERE ts IS NOT NULL
    )
    SELECT 
        usc.userId,
        usc.sessionId,
        usc.channel,
        st.ts AS session_timestamp,
        DATE_TRUNC('WEEK', st.ts) AS session_week
    FROM dev.raw_data.user_session_channel usc
    JOIN cleaned_sessions st ON usc.sessionId = st.sessionId
    WHERE st.row_num = 1;  -- Keep only the first occurrence of each sessionId
    """,
    snowflake_conn_id='snowflake_conn',
    dag=data_processing_dag
)

create_schema_task >> generate_session_summary
