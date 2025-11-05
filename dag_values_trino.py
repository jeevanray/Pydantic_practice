from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
import time
import json

# MinIO configuration
minio_endpoint = 'minio-service:9000'  # Update with your MinIO service endpoint
minio_access_key = 'your-access-key'  # Update with your credentials
minio_secret_key = 'your-secret-key'  # Update with your credentials
minio_bucket = 'hclsw-hss-prd-cdp-app'
minio_secure = False  # Set to True if using HTTPS

# Trino configuration
trino_host = 'trino-coordinator'  # Update with your Trino coordinator service
trino_port = 8080
trino_user = 'airflow'
trino_catalog = 'hive'  # or 'iceberg' depending on your setup

tenant_id = '6551'
output_path = f'athena-folder-query/{tenant_id}/'
original_table = "dimensionvalues"
temp_table = "temp"
db_name = f'segmentdb_{tenant_id}'

def get_minio_client():
    """Initialize MinIO client"""
    return Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure
    )

def get_trino_connection():
    """Initialize Trino connection"""
    return connect(
        host=trino_host,
        port=trino_port,
        user=trino_user,
        catalog=trino_catalog,
        schema=db_name,
        # auth=BasicAuthentication(trino_user, 'password')  # Uncomment if auth is needed
    )

def getcurrenttimestamp():
    """Get current timestamp in milliseconds"""
    timestamp_in_milliseconds = int(time.time() * 1000)
    timestamp_str = str(timestamp_in_milliseconds)
    print(f"Generated timestamp: {timestamp_str}")
    return timestamp_str

def execute_trino_query(query: str, fetch_results=False):
    """Execute a Trino query and optionally fetch results"""
    conn = get_trino_connection()
    cursor = conn.cursor()
    
    try:
        print(f"Executing query: {query}")
        cursor.execute(query)
        
        if fetch_results:
            # Fetch column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            result_list = []
            for row in rows:
                row_dict = {columns[i]: row[i] for i in range(len(columns))}
                result_list.append(row_dict)
            
            print(f"Query returned {len(result_list)} rows")
            return result_list
        else:
            # For non-SELECT queries, just wait for completion
            cursor.fetchall()  # Consume results
            print("Query executed successfully")
            return None
            
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

def getResult(query: str):
    """Wrapper function for fetching query results"""
    return execute_trino_query(query, fetch_results=True)

def build_query(tablename, location):
    """Build CREATE TABLE query for Trino/Hive"""
    # Convert S3 path to MinIO path
    minio_location = f's3a://{minio_bucket}/{output_path}{location.split("/")[-2]}/'
    
    query = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{tablename} (
            tenant VARCHAR,
            tablename VARCHAR,
            column_name VARCHAR,
            distinctvalues VARCHAR
        )
        WITH (
            format = 'PARQUET',
            external_location = '{minio_location}'
        )
    """
    
    print(f"Created table query: {query}")
    return query

def build_insert_query(tablename, values):
    """Build INSERT query for regular columns"""
    query = f"""
        INSERT INTO {db_name}.{tablename} 
        SELECT 
            '{tenant_id}' as tenant,
            '{values['tablecollectionname']}' as tablename,
            '{values['dimension']}' as column_name,
            CAST({values['dimension']} AS VARCHAR) as distinctvalues
        FROM {db_name}."{values['tablecollectionname']}"
        GROUP BY {values['dimension']}
        LIMIT 1000
    """
    
    return query

def build_insert_array_query(tablename, values):
    """Build INSERT query for array columns"""
    query = f"""
        INSERT INTO {db_name}.{tablename}
        SELECT 
            '{tenant_id}',
            '{values['tablecollectionname']}',
            '{values['dimension']}',
            CONCAT('[', ARRAY_JOIN(
                TRANSFORM(
                    ARRAY_AGG(DISTINCT elem), 
                    x -> CONCAT('''', x, '''')
                ), 
                ', '
            ), ']')
        FROM {db_name}."{values['tablecollectionname']}"
        CROSS JOIN UNNEST({values['dimension']}) AS t(elem)
    """
    
    return query

def process_inserts(**context):
    """Process all insert queries from metadata"""
    metadata = context['task_instance'].xcom_pull(task_ids='get_trino_metadata')
    
    if not metadata:
        print("No metadata found to process")
        return
    
    print(f"Processing {len(metadata)} insert queries")
    
    for entry in metadata:
        try:
            if entry.get('datatype') == 'array(varchar)' or entry.get('datatype') == 'array<string>':
                query = build_insert_array_query(temp_table, entry)
            else:
                query = build_insert_query(temp_table, entry)
            
            execute_trino_query(query, fetch_results=False)
            print(f"Successfully inserted data for {entry['dimension']}")
            
        except Exception as e:
            print(f"Error processing insert for {entry['dimension']}: {str(e)}")
            raise

def execute_query_task(query, **context):
    """Generic function to execute a Trino query"""
    # Resolve any Jinja templating
    if '{{' in query:
        ti = context['task_instance']
        # Replace XCom pulls
        if 'get_current_timestamp_task' in query:
            timestamp = ti.xcom_pull(task_ids='get_current_timestamp_task')
            query = query.replace('{{ task_instance.xcom_pull(task_ids="get_current_timestamp_task") }}', timestamp)
        if 'build_athena_query_original_table' in query:
            build_query_result = ti.xcom_pull(task_ids='build_athena_query_original_table')
            query = build_query_result
        if 'build_athena_query_temp_table' in query:
            build_query_result = ti.xcom_pull(task_ids='build_athena_query_temp_table')
            query = build_query_result
    
    execute_trino_query(query, fetch_results=False)

with DAG(
    dag_id='get_distinct_values_DAG_6551_trino',
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 19),
    catchup=False,
) as dag:

    get_metadata = PythonOperator(
        task_id='get_trino_metadata',
        python_callable=getResult,
        op_kwargs={
            'query': f'SELECT DISTINCT tablecollectionname, dimension, datatype FROM {db_name}.segmentationmetadata WHERE isenumerable = true'
        },
        provide_context=True,
        dag=dag
    )

    get_current_timestamp = PythonOperator(
        task_id='get_current_timestamp_task',
        python_callable=getcurrenttimestamp,
        dag=dag
    )

    build_query_task_original_table = PythonOperator(
        task_id='build_trino_query_original_table',
        python_callable=build_query,
        op_kwargs={
            'tablename': original_table,
            'location': 'v1/'
        },
        provide_context=True,
        dag=dag
    )

    run_trino_query_original = PythonOperator(
        task_id='execute_query_original',
        python_callable=execute_query_task,
        op_kwargs={'query': "{{ task_instance.xcom_pull(task_ids='build_trino_query_original_table') }}"},
        provide_context=True,
        dag=dag
    )

    build_query_task_temp_table = PythonOperator(
        task_id='build_trino_query_temp_table',
        python_callable=build_query,
        op_kwargs={
            'tablename': temp_table,
            'location': '{{ task_instance.xcom_pull(task_ids="get_current_timestamp_task") }}/'
        },
        provide_context=True,
        dag=dag
    )

    run_trino_query_temp = PythonOperator(
        task_id='execute_query_temp',
        python_callable=execute_query_task,
        op_kwargs={'query': "{{ task_instance.xcom_pull(task_ids='build_trino_query_temp_table') }}"},
        provide_context=True,
        dag=dag
    )

    process_inserts_task = PythonOperator(
        task_id='process_inserts',
        python_callable=process_inserts,
        provide_context=True,
        dag=dag
    )

    alter_trino_original_table = PythonOperator(
        task_id='alter_trino_original_table_loc',
        python_callable=execute_query_task,
        op_kwargs={
            'query': f"""ALTER TABLE {db_name}.{original_table} SET LOCATION 's3a://{minio_bucket}/{output_path}""" + 
                     '{{ task_instance.xcom_pull(task_ids="get_current_timestamp_task") }}' + """/'"""
        },
        provide_context=True,
        trigger_rule='all_success',
        dag=dag,
    )

    drop_trino_temp_table = PythonOperator(
        task_id='drop_trino_temp_table',
        python_callable=execute_query_task,
        op_kwargs={
            'query': f"""DROP TABLE IF EXISTS {db_name}.{temp_table}"""
        },
        provide_context=True,
        dag=dag,
    )

    # Define task dependencies
    [get_metadata, get_current_timestamp] >> build_query_task_original_table >> run_trino_query_original >> \
    build_query_task_temp_table >> run_trino_query_temp >> process_inserts_task >> \
    alter_trino_original_table >> drop_trino_temp_table
