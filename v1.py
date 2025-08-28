import io
import pandas as pd
import oracledb
from typing import Dict, Any
from minio_handler import MinioHandler
from datetime import datetime, timedelta
import pytz
import logging
import time


# Set up logging
logger = logging.getLogger(__name__)


# Constants (assuming these are defined elsewhere in your codebase)
DATETIMEFORMAT = '%Y-%m-%d %H:%M:%S'
ORACLE_DATE = 'YYYY-MM-DD'


def connect_to_oracle(oracle_conf):
    """
    Connect to Oracle database with retry logic
    """
    logger.debug("Connecting to Oracle Db")
    attempt = 0
    while attempt < 3:
        try:
            connection = oracledb.connect(
                user=oracle_conf["user"],
                password=oracle_conf["password"],
                dsn=oracle_conf["dsn"],
            )
            cursor = connection.cursor()
            logger.debug("Connected to Oracle DB successfully.")
            return connection, cursor
        except Exception as e:
            attempt += 1
            logger.error(
                f"[Connection Attempt {attempt}] Failed to connect to Oracle DB: {str(e)}"
            )
            time.sleep(5)
            if attempt == 3:
                raise e


def close_connection(cursor, connection):
    """
    Close database connections safely
    """
    try:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        logger.info("Connection to OracleDb closed")
    except Exception as e:
        logger.error(f"Error occurred when closing the OracleDb connection - {e}")


def prepare_auditing():
    """
    The method is used to reset the auditing log at the start of each/new
    delta date
    Arguments:
        None
    Returns:
        Dict - Reset audit log
    """
    return {
        "source_table": "",
        "task_startts": "",
        "task_endts": "",
        "task_exec_secs": 0,
        "business_loaddt": "",
        "total_records": 0,
        "extraction_time": 0,
        "total_apicalls": 0,
        "success_apicalls": 0,
        "failed_apicalls": 0,
        "api_failedpath": "",
        "apicall_time": 0,
        "cdp_db_count_validation": False,
        "aerospike_init_record_cnt": 0,
        "aerospike_init_read_waittime": 0,
        "aerospike_record_cnt": 0,
        "aerospike_waittime": 0,
        "aerospike_error": "",
        "mongodb_init_record_cnt": 0,
        "mongodb_record_cnt": 0,
        "mongodb_init_read_waittime": 0,
        "mongodb_waittime": 0,
        "mongodb_error": "",
        "suspected_updates_or_blacklisted_records": 0,
        "difference_aero_mongo": 0,
        "status": "",
        "log_path": "",
        "minio_filepath": "",
        "restart_point": 0,
    }


def initialize_restart_audit_log(config_audit, audit_log, aud_dt):
    """
    Initialize audit log parameters for restart functionality
    """
    query = f"""
                SELECT
                    source_table,
                    task_startts,
                    task_endts,
                    task_exec_secs,
                    business_loaddt,
                    total_records,
                    extraction_time,
                    total_apicalls,
                    success_apicalls,
                    failed_apicalls,
                    api_failedpath,
                    apicall_time,
                    cdp_db_count_validation,
                    aerospike_init_record_cnt,
                    aerospike_init_read_waittime,
                    aerospike_record_cnt,
                    aerospike_waittime,
                    aerospike_error,
                    mongodb_init_record_cnt,
                    mongodb_record_cnt,
                    mongodb_init_read_waittime,
                    mongodb_waittime,
                    mongodb_error,
                    suspected_updates_or_blacklisted_records,
                    difference_aero_mongo,
                    status,
                    log_path,
                    minio_filepath,
                    restart_point
                FROM {config_audit["schema"]}.{config_audit["audit_table"]}
                WHERE BUSINESS_LOADDT = TO_DATE('{aud_dt}', '{ORACLE_DATE}')
                  AND SOURCE_TABLE = '{audit_log["source_table"]}'
            """
    connection, cursor = connect_to_oracle(config_audit["target"])
    try:
        logger.debug(f'Executing query - {query}, {aud_dt}')
        cursor.execute(query)
        row = cursor.fetchone()
        if row:
            keys = list(audit_log.keys())
            audit_log.update(dict(zip(keys, row)))
            if isinstance(audit_log.get("api_failedpath"), oracledb.LOB):
                audit_log["api_failedpath"] = audit_log["api_failedpath"].read()
            logger.info(f'Audit Logger initialized for restart - Status: {audit_log["status"]}, Restart Point: {audit_log["restart_point"]}')
        else:
            logger.info('No previous audit record found - starting fresh')
    except Exception as e:
        logger.error(f"Failed to initialize audit record: {e}")
        raise e
    finally:
        close_connection(cursor, connection)


def update_audit_record(config_audit, audit_data):
    """
    Upserts a record into the audit table based on (source_table, business_loaddt).
    """
    # Convert boolean to string AND time fields casting
    audit_data['cdp_db_count_validation'] = 'Y' if audit_data['cdp_db_count_validation'] else 'N'
    
    # Handle datetime conversions safely
    if isinstance(audit_data['task_startts'], str) and audit_data['task_startts']:
        audit_data['task_startts'] = datetime.strptime(audit_data['task_startts'], DATETIMEFORMAT)
    if isinstance(audit_data['task_endts'], str) and audit_data['task_endts']:
        audit_data['task_endts'] = datetime.strptime(audit_data['task_endts'], DATETIMEFORMAT)
    if isinstance(audit_data['business_loaddt'], str):
        audit_data['business_loaddt'] = datetime.strptime(audit_data['business_loaddt'], '%Y-%m-%d')
    
    # Handle long api_failedpath
    if audit_data['api_failedpath'] is not None and len(str(audit_data['api_failedpath'])) > 7900:
        logger.info('Length of api_failedpath exceeding Oracle clob size, truncating to base location')
        audit_data['api_failedpath'] = config_audit.get('output_file_path', 'Base location - check logs')
    
    # Set timestamps
    updated_at_ts = datetime.now(pytz.timezone("Asia/Kolkata")).strftime(DATETIMEFORMAT)
    audit_data["updated_at_ts"] = datetime.strptime(updated_at_ts, DATETIMEFORMAT)
    
    # Only set created_at_ts if it's a new record (i.e., not a restart)
    if not audit_data.get("created_at_ts"):
        audit_data["created_at_ts"] = audit_data["updated_at_ts"]

    merge_sql = f"""
        MERGE /*+ PARALLEL(target, 4) */ INTO {config_audit['schema']}.{config_audit['audit_table']} target
        USING (
            SELECT
                :source_table AS source_table,
                :business_loaddt AS business_loaddt
            FROM dual
        ) src
        ON (
            target.source_table = src.source_table AND
            target.business_loaddt = src.business_loaddt
        )
        WHEN MATCHED THEN
            UPDATE SET
                task_startts = :task_startts,
                task_endts = :task_endts,
                task_exec_secs = :task_exec_secs,
                total_records = :total_records,
                extraction_time = :extraction_time,
                total_apicalls = :total_apicalls,
                success_apicalls = :success_apicalls,
                failed_apicalls = :failed_apicalls,
                api_failedpath = :api_failedpath,
                apicall_time = :apicall_time,
                cdp_db_count_validation = :cdp_db_count_validation,
                aerospike_init_record_cnt = :aerospike_init_record_cnt,
                aerospike_init_read_waittime = :aerospike_init_read_waittime,
                aerospike_record_cnt = :aerospike_record_cnt,
                aerospike_waittime = :aerospike_waittime,
                aerospike_error = :aerospike_error,
                mongodb_init_record_cnt = :mongodb_init_record_cnt,
                mongodb_record_cnt = :mongodb_record_cnt,
                mongodb_init_read_waittime = :mongodb_init_read_waittime,
                mongodb_waittime = :mongodb_waittime,
                mongodb_error = :mongodb_error,
                suspected_updates_or_blacklisted_records = :suspected_updates_or_blacklisted_records,
                difference_aero_mongo = :difference_aero_mongo,
                status = :status,
                log_path = :log_path,
                minio_filepath = :minio_filepath,
                restart_point = :restart_point,
                updated_at_ts = :updated_at_ts
        WHEN NOT MATCHED THEN
            INSERT (
                source_table,
                business_loaddt,
                task_startts,
                task_endts,
                task_exec_secs,
                total_records,
                extraction_time,
                total_apicalls,
                success_apicalls,
                failed_apicalls,
                api_failedpath,
                apicall_time,
                cdp_db_count_validation,
                aerospike_init_record_cnt,
                aerospike_init_read_waittime,
                aerospike_record_cnt,
                aerospike_waittime,
                aerospike_error,
                mongodb_init_record_cnt,
                mongodb_record_cnt,
                mongodb_init_read_waittime,
                mongodb_waittime,
                mongodb_error,
                suspected_updates_or_blacklisted_records,
                difference_aero_mongo,
                status,
                log_path,
                minio_filepath,
                restart_point,
                created_at_ts,
                updated_at_ts
            )
            VALUES (
                :source_table,
                :business_loaddt,
                :task_startts,
                :task_endts,
                :task_exec_secs,
                :total_records,
                :extraction_time,
                :total_apicalls,
                :success_apicalls,
                :failed_apicalls,
                :api_failedpath,
                :apicall_time,
                :cdp_db_count_validation,
                :aerospike_init_record_cnt,
                :aerospike_init_read_waittime,
                :aerospike_record_cnt,
                :aerospike_waittime,
                :aerospike_error,
                :mongodb_init_record_cnt,
                :mongodb_record_cnt,
                :mongodb_init_read_waittime,
                :mongodb_waittime,
                :mongodb_error,
                :suspected_updates_or_blacklisted_records,
                :difference_aero_mongo,
                :status,
                :log_path,
                :minio_filepath,
                :restart_point,
                :created_at_ts,
                :updated_at_ts
            )
    """

    connection, cursor = connect_to_oracle(config_audit["target"])
    try:
        logger.debug(f'Executing merge query for table: {audit_data["source_table"]}, business_loaddt: {audit_data["business_loaddt"]}')
        cursor.execute(merge_sql, audit_data)
        connection.commit()
        logger.info(f"Audit record upserted for: {audit_data['source_table']} | {audit_data['business_loaddt']} | Status: {audit_data['status']}")
    except Exception as e:
        logger.error(f"Failed to upsert audit record: {e}")
        connection.rollback()
        raise e
    finally:
        close_connection(cursor, connection)


def get_load_status_and_dates(config_audit, source_table, current_business_loaddt):
    """
    Gets all dates that need to be processed and their current status.
    Returns list of tuples: (date, status, restart_point, total_records)
    """
    # Get all dates that need processing (failed, running, or missing)
    query = f"""
        WITH date_range AS (
            -- Get all dates from source table that need processing
            SELECT DISTINCT business_loaddt
            FROM {config_audit["schema"]}.{source_table}
            WHERE business_loaddt <= TO_DATE('{current_business_loaddt}', '{ORACLE_DATE}')
        ),
        audit_status AS (
            -- Get current audit status for each date
            SELECT 
                business_loaddt,
                status,
                restart_point,
                total_records
            FROM {config_audit["schema"]}.{config_audit["audit_table"]}
            WHERE source_table = '{source_table}'
        )
        SELECT 
            dr.business_loaddt,
            NVL(aus.status, 'NOT_STARTED') as status,
            NVL(aus.restart_point, 0) as restart_point,
            NVL(aus.total_records, 0) as total_records
        FROM date_range dr
        LEFT JOIN audit_status aus ON dr.business_loaddt = aus.business_loaddt
        WHERE NVL(aus.status, 'NOT_STARTED') IN ('NOT_STARTED', 'FAILED', 'RUNNING')
        ORDER BY dr.business_loaddt
    """
    
    connection, cursor = connect_to_oracle(config_audit["target"])
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        processed_dates = []
        for row in results:
            business_dt, status, restart_point, total_records = row
            processed_dates.append({
                'business_loaddt': business_dt.strftime('%Y-%m-%d') if isinstance(business_dt, datetime) else business_dt,
                'status': status,
                'restart_point': restart_point,
                'total_records': total_records
            })
            
        logger.info(f"Found {len(processed_dates)} dates that need processing for {source_table}")
        for date_info in processed_dates:
            logger.info(f"  - {date_info['business_loaddt']}: Status={date_info['status']}, Restart Point={date_info['restart_point']}")
            
        return processed_dates
        
    except Exception as e:
        logger.error(f"Failed to get load status and dates: {e}")
        raise e
    finally:
        close_connection(cursor, connection)


def create_audit_table_if_not_exists(config_audit):
    """
    Create audit table if it doesn't exist
    """
    check_query = f"""
        SELECT COUNT(*)
        FROM all_tables 
        WHERE table_name = UPPER('{config_audit['audit_table']}')
              AND owner = UPPER('{config_audit['schema']}')
    """
    
    connection, cursor = connect_to_oracle(config_audit["target"])
    try:
        cursor.execute(check_query)
        exists = cursor.fetchone()[0]

        if exists == 0:
            logger.warning(f"Audit table '{config_audit['audit_table']}' does not exist. Creating it now...")
            create_table_query = f"""
                CREATE TABLE {config_audit['schema']}.{config_audit['audit_table']} (
                    run_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    source_table VARCHAR2(100),
                    task_startts TIMESTAMP,
                    task_endts TIMESTAMP,
                    task_exec_secs NUMBER,
                    business_loaddt DATE,
                    total_records NUMBER,
                    extraction_time NUMBER,
                    total_apicalls NUMBER,
                    success_apicalls NUMBER,
                    failed_apicalls NUMBER,
                    api_failedpath CLOB,
                    apicall_time NUMBER,
                    cdp_db_count_validation VARCHAR2(10),
                    aerospike_init_record_cnt NUMBER,
                    aerospike_init_read_waittime NUMBER,
                    aerospike_record_cnt NUMBER,
                    aerospike_waittime NUMBER,
                    aerospike_error CLOB,
                    mongodb_init_record_cnt NUMBER,
                    mongodb_record_cnt NUMBER,
                    mongodb_init_read_waittime NUMBER,
                    mongodb_waittime NUMBER,
                    mongodb_error CLOB,
                    suspected_updates_or_blacklisted_records NUMBER,
                    difference_aero_mongo NUMBER,
                    status VARCHAR2(50),
                    log_path VARCHAR2(1000),
                    minio_filepath VARCHAR2(1000),
                    restart_point NUMBER DEFAULT 0,
                    created_at_ts TIMESTAMP,
                    updated_at_ts TIMESTAMP,
                    CONSTRAINT uq_source_bizdate UNIQUE (source_table, business_loaddt)
                )
            """
            cursor.execute(create_table_query)
            connection.commit()
            logger.info("Audit table created successfully.")
        else:
            logger.info("Audit table already exists.")
            
    except Exception as e:
        logger.error(f"Failed to create audit table: {e}")
        raise e
    finally:
        close_connection(cursor, connection)


def oracle_to_minio_parquet(
    oracle_config: Dict[str, Any],
    minio_config: Dict[str, Any],
    config_audit: Dict[str, Any],
    table_name: str,
    base_object_path: str,
    business_loaddt: str,
    chunk_size: int = 10000,
    compression: str = 'snappy',
    restart_point: int = 0
):
    """
    Extracts data from an Oracle database table in chunks and uploads it to
    a MinIO bucket in Parquet format with restartability.

    Args:
        oracle_config (Dict[str, Any]): Oracle connection configuration.
        minio_config (Dict[str, Any]): MinIO connection configuration.
        config_audit (Dict[str, Any]): Audit table configuration (schema, audit_table, target).
        table_name (str): The Oracle database table name to extract data from.
        base_object_path (str): The base object path in MinIO for the Parquet files.
        business_loaddt (str): Business load date (e.g., '2025-08-28') for audit tracking.
        chunk_size (int): The number of rows to fetch per database round-trip.
        compression (str): The compression type for Parquet files (e.g., 'snappy').
        restart_point (int): The chunk index to restart from (for failed runs).
    """
    
    extraction_start_time = datetime.now()
    
    try:
        # Ensure audit table exists
        create_audit_table_if_not_exists(config_audit)

        # Parse business_loaddt to datetime and format to ddmmyyyy
        load_dt = datetime.strptime(business_loaddt, '%Y-%m-%d')
        date_folder = load_dt.strftime('%d%m%Y')
        effective_object_path = f"{base_object_path}/{date_folder}"

        # Initialize audit log
        audit_log = prepare_auditing()
        audit_log.update({
            "source_table": table_name,
            "business_loaddt": business_loaddt,
            "task_startts": datetime.now(pytz.timezone("Asia/Kolkata")).strftime(DATETIMEFORMAT),
            "status": "RUNNING",
            "log_path": config_audit.get('output_file_path', base_object_path),
            "minio_filepath": effective_object_path,
            "restart_point": restart_point
        })

        # Check for existing audit record to support restart
        initialize_restart_audit_log(config_audit, audit_log, business_loaddt)

        # Use the higher restart point (either passed or from audit log)
        start_chunk_index = max(restart_point, audit_log.get("restart_point", 0))
        total_records_processed = audit_log.get("total_records", 0)
        
        logger.info(f"Starting extraction for {table_name} on {business_loaddt}")
        logger.info(f"Restart point: {start_chunk_index}, Records processed so far: {total_records_processed}")

        # Connect to Oracle and process data
        connection, cursor = connect_to_oracle(oracle_config)
        try:
            # Construct and execute the SQL query with OFFSET for restart capability
            if start_chunk_index > 0:
                offset_rows = start_chunk_index * chunk_size
                sql_query = f"""
                    SELECT * FROM (
                        SELECT /*+ PARALLEL(target, 4) */ *, ROW_NUMBER() OVER (ORDER BY ROWID) as rn 
                        FROM {table_name}
                    ) WHERE rn > {offset_rows}
                """
                logger.info(f"Restarting from offset: {offset_rows} rows")
            else:
                sql_query = f"SELECT /*+ PARALLEL(target, 4) */ * FROM {table_name}"
                logger.info("Starting fresh extraction")
            
            cursor.arraysize = chunk_size
            cursor.execute(sql_query)

            # Get column names (filter out ROW_NUMBER column if present)
            columns = [desc[0] for desc in cursor.description if desc[0] != 'RN']
            logger.info(f"Columns found: {len(columns)}")

            # Initialize MinIO connection
            minio_client = MinioHandler(minio_config)
            minio_client.__enter__()
            
            try:
                chunk_index = start_chunk_index
                while True:
                    rows_chunk = cursor.fetchmany()
                    if not rows_chunk:
                        logger.info("No more data to process")
                        break

                    # Remove the RN column if it exists (from ROW_NUMBER())
                    if sql_query.find('ROW_NUMBER()') != -1:
                        rows_chunk = [row[:-1] for row in rows_chunk]  # Remove last column (RN)

                    # Convert the chunk of rows to a pandas DataFrame
                    df_chunk = pd.DataFrame(rows_chunk, columns=columns)
                    chunk_records = len(df_chunk)

                    # Create a unique object key for each chunk using effective path
                    part_object_key = f"{effective_object_path}_{chunk_index:06d}.parquet"

                    # Upload to MinIO
                    minio_client._upload_dataframe_single(
                        df=df_chunk,
                        object_path=part_object_key,
                        format="parquet",
                        compression=compression
                    )
                    logger.info(f"Uploaded chunk {chunk_index} with {chunk_records} records to {part_object_key}")

                    # Update audit log metrics
                    total_records_processed += chunk_records
                    current_time = datetime.now()
                    
                    audit_log.update({
                        "total_records": total_records_processed,
                        "status": "RUNNING",
                        "task_endts": current_time.astimezone(pytz.timezone("Asia/Kolkata")).strftime(DATETIMEFORMAT),
                        "task_exec_secs": (current_time - extraction_start_time).total_seconds(),
                        "extraction_time": (current_time - extraction_start_time).total_seconds(),
                        "restart_point": chunk_index + 1  # Set to next chunk for restart
                    })

                    # Update audit record after each chunk
                    update_audit_record(config_audit, audit_log)

                    chunk_index += 1

                # Final audit update on success
                final_time = datetime.now()
                audit_log.update({
                    "status": "COMPLETED",
                    "task_endts": final_time.astimezone(pytz.timezone("Asia/Kolkata")).strftime(DATETIMEFORMAT),
                    "task_exec_secs": (final_time - extraction_start_time).total_seconds(),
                    "extraction_time": (final_time - extraction_start_time).total_seconds()
                })
                
                update_audit_record(config_audit, audit_log)
                logger.info("Oracle data successfully extracted and saved to MinIO in Parquet format.")

            finally:
                minio_client.__exit__(None, None, None)
                    
        finally:
            close_connection(cursor, connection)

    except Exception as e:
        # Update audit log with failure status
        final_time = datetime.now()
        audit_log.update({
            "status": "FAILED",
            "task_endts": final_time.astimezone(pytz.timezone("Asia/Kolkata")).strftime(DATETIMEFORMAT),
            "task_exec_secs": (final_time - extraction_start_time).total_seconds(),
            "aerospike_error": str(e)  # Using aerospike_error as a generic error field
        })
        
        try:
            update_audit_record(config_audit, audit_log)
        except Exception as audit_error:
            logger.error(f"Failed to update audit record on failure: {audit_error}")
            
        logger.error(f"An error occurred during the Oracle to MinIO transfer: {e}")
        raise


def process_oracle_to_minio_with_dependencies(
    oracle_config: Dict[str, Any],
    minio_config: Dict[str, Any],
    config_audit: Dict[str, Any],
    table_name: str,
    base_object_path: str,
    current_business_loaddt: str,
    chunk_size: int = 10000,
    compression: str = 'snappy'
):
    """
    Main function to process Oracle to MinIO with dependency checking and retry logic.
    
    This function:
    1. Gets all dates that need processing (failed, running, or missing)
    2. Processes them in chronological order
    3. Handles retry logic for failed loads
    4. Skips running loads unless they're actually failed
    5. Supports restart from checkpoint for failed loads
    """
    
    logger.info(f"Starting Oracle to MinIO processing for {table_name} up to {current_business_loaddt}")
    
    # Get all dates that need processing
    dates_to_process = get_load_status_and_dates(config_audit, table_name, current_business_loaddt)
    
    if not dates_to_process:
        logger.info("No dates need processing. All loads are completed.")
        return
    
    # Process each date in chronological order
    for date_info in dates_to_process:
        business_dt = date_info['business_loaddt']
        status = date_info['status']
        restart_point = date_info['restart_point']
        
        logger.info(f"Processing date {business_dt} with status: {status}")
        
        try:
            if status == 'RUNNING':
                # Check if it's actually running or just stale
                # For now, we'll consider any RUNNING status as potentially stale and allow restart
                logger.warning(f"Date {business_dt} is in RUNNING status. Treating as FAILED and restarting from checkpoint.")
                status = 'FAILED'
            
            if status in ['NOT_STARTED', 'FAILED']:
                # Determine restart point
                effective_restart_point = restart_point if status == 'FAILED' else 0
                
                logger.info(f"{'Restarting' if status == 'FAILED' else 'Starting'} load for {business_dt} from restart point: {effective_restart_point}")
                
                # Run the extraction
                oracle_to_minio_parquet(
                    oracle_config=oracle_config,
                    minio_config=minio_config,
                    config_audit=config_audit,
                    table_name=table_name,
                    base_object_path=base_object_path,
                    business_loaddt=business_dt,
                    chunk_size=chunk_size,
                    compression=compression,
                    restart_point=effective_restart_point
                )
                
                logger.info(f"Successfully completed load for {business_dt}")
            
            elif status == 'COMPLETED':
                logger.info(f"Date {business_dt} already completed. Skipping.")
            
        except Exception as e:
            logger.error(f"Failed to process date {business_dt}: {str(e)}")
            # Continue with next date instead of stopping completely
            continue
    
    logger.info("Completed Oracle to MinIO processing for all required dates.")


### Example usage with configuration
if __name__ == "__main__":
    oracle_connection_config = {
        "user": "uds",
        "password": "P*03",
        "dsn": "ora-scn-pr.dwhmartr.bank:21521/madwhprd"
    }

    minio_connection_config = {
        "endpoint": "minio-cdp-prod.apps.ocpdwhp.dwhmartr.bank.sbi",
        "access_key": "xvMjyTKmmWwbr6Ha",
        "secret_key": "ERpzo3YbhfSPedY1acb8swUdVhLEcy2K",
        "bucket_name": "sbi-test",
        "secure": True
    }

    audit_config = {
        "target": {
            "user": "uds",
            "password": "P*03",
            "dsn": "ora-scn-pr.dwhmartr.bank:21521/madwhprd"
        },
        "schema": "cdp_unica_refined",
        "audit_table": "audit_log_table",
        "output_file_path": "/path/to/logs"
    }

    table_to_extract = "cdp_unica_refined.customer_profile"
    minio_output_path = "segmentation/segmentdb_6550/input_data/customer_data/customer_profile"
    current_business_load_date = "2025-08-28"

    # Process with dependency checking and retry logic
    process_oracle_to_minio_with_dependencies(
        oracle_config=oracle_connection_config,
        minio_config=minio_connection_config,
        config_audit=audit_config,
        table_name=table_to_extract,
        base_object_path=minio_output_path,
        current_business_loaddt=current_business_load_date,
        chunk_size=100_000,
        compression='snappy'
    )
