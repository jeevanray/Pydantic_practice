import os
import logging
from typing import Dict, Any, List, Optional, Union, Sequence, Tuple
from datetime import datetime
import datetime

import pandas as pd
import oracledb
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utilities import connect_to_oracle, close_connection
from constants import ORACLE_DATE, DATETIMEFORMAT

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def get_historic_load_status(config_audit: Dict[str, Any], source_table: str,
                           current_business_loaddt: str, delta_column: str,
                           delta_column_type: str, delta_column_format:str,
                           oracle_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get historic load status - uses pandas for READING (allowed)."""
    file_name = f'source_delta_{source_table.replace(".", "_")}.parquet'
    parquet_basepath = "/opt/airflow/etl_inward_files/CDP_minio/"
    parquet_file = os.path.join(parquet_basepath, file_name)
    os.makedirs(parquet_basepath, exist_ok=True)

    try:
        # Load distinct delta values from Parquet or refresh from source
        with connect_to_oracle(oracle_config) as source_conn:
            if not os.path.exists(parquet_file):
                logger.info("Parquet file %s not found. Refreshing all distinct delta values.", parquet_file)
                query = f"SELECT /*+ PARALLEL(4) */ DISTINCT {delta_column} as delta_value FROM {source_table} WHERE {delta_column} IS NOT NULL ORDER BY {delta_column}"
                source_deltas_df = pd.read_sql(query, source_conn)
                if delta_column_type:
                    if delta_column_type.lower() == 'timestamp':
                        source_deltas_df['DELTA_VALUE'] = (pd.to_datetime(source_deltas_df['DELTA_VALUE'],
                                                                          format=delta_column_format).dt.strftime('%Y-%m-%d'))
                    elif delta_column_type.lower() == 'date' and delta_column_format is not None:
                        source_deltas_df['DELTA_VALUE'] = (pd.to_datetime(source_deltas_df['DELTA_VALUE'],
                                                            format=delta_column_format).dt.strftime('%Y-%m-%d')).drop_duplicates()
                source_deltas_df.to_parquet(parquet_file, index=False)
                logger.info("Saved refreshed distinct delta values to %s.", parquet_file)
            else:
                logger.debug("Using cached parquet file %s", parquet_file)
                source_deltas_df = pd.read_parquet(parquet_file)  # PANDAS READING - ALLOWED

        all_deltas = source_deltas_df['DELTA_VALUE'].astype(str).tolist()
        last_delta = all_deltas[-1]
        if not all_deltas:
            logger.info("No delta values found for %s in Parquet file.", source_table)
            return []

        # Get audit record for completed deltas and status
        with connect_to_oracle(config_audit["target"]) as audit_conn:
            processed_deltas_query = f"""
                SELECT
                    business_loaddt,
                    NVL(status, 'NOT_STARTED') AS status,
                    NVL(restart_point, 0) AS restart_point,
                    NVL(total_records, 0) AS total_records,
                    delta_column_value,
                    NVL(load_type, 'historic') AS load_type,
                    NVL(task_exec_secs, 0) AS task_exec_secs,
                    NVL(extraction_time, 0) AS extraction_time,
                    aerospike_error
                FROM {config_audit["schema"]}.{config_audit["audit_table"]}
                WHERE source_table = :src
                AND load_type = 'historic'
                ORDER BY updated_at_ts DESC NULLS LAST
                FETCH FIRST 1 ROW ONLY
            """
            cur = audit_conn.cursor()
            cur.execute(processed_deltas_query, {"src": source_table})
            row = cur.fetchone()

            base_status = "NOT_STARTED"
            base_restart_point = 0
            base_total_records = 0
            base_exec_secs = 0
            base_extraction_time = 0
            base_delta_value = None
            biz_str = current_business_loaddt

            if row:
                business_dt, status, restart_point, total_records, delta_val, load_type, task_exec_secs, extraction_time, aerospike_error = row
                if isinstance(aerospike_error, oracledb.LOB):
                    aerospike_error = aerospike_error.read()
                base_status = status
                base_restart_point = int(restart_point or 0)
                base_total_records = int(total_records or 0)
                base_exec_secs = float(task_exec_secs or 0)
                base_extraction_time = float(extraction_time or 0)
                base_delta_value = delta_val
                biz_str = (business_dt.strftime("%Y-%m-%d") if hasattr(business_dt, "strftime") else str(business_dt)) if business_dt else current_business_loaddt

            # Find unprocessed deltas
            try:
                processed_index = all_deltas.index(base_delta_value) if base_delta_value else -1
            except ValueError:
                processed_index = -1
            unprocessed_deltas = all_deltas[processed_index+1:]

            if last_delta not in unprocessed_deltas:
                try:
                    with connect_to_oracle(oracle_config) as source_conn:
                            logger.info("Parquet file %s outdated. Refreshing all distinct delta values.", parquet_file)
                            query = f"SELECT /*+ PARALLEL(4) */ DISTINCT {delta_column} as delta_value FROM {source_table} WHERE {delta_column} IS NOT NULL ORDER BY {delta_column}"
                            source_deltas_df = pd.read_sql(query, source_conn)
                            if delta_column_type:
                                if delta_column_type.lower() == 'timestamp':
                                    source_deltas_df['DELTA_VALUE'] = (pd.to_datetime(source_deltas_df['DELTA_VALUE'],
                                                                                    format=delta_column_format).dt.strftime('%Y-%m-%d')).drop_duplicates()
                            source_deltas_df.to_parquet(parquet_file, index=False)
                            logger.info("Saved refreshed distinct delta values to %s.", parquet_file)

                except Exception  as e:
                    logger.error("Could not refresh the parquet file for delta_value")
                    raise e

            if not unprocessed_deltas:
                logger.info("All historic deltas completed for %s", source_table)
                return []

            logger.info("Found %s unprocessed deltas for processing", len(unprocessed_deltas))

            # Create job entries
            processed_jobs: List[Dict[str, Any]] = []
            for delta in unprocessed_deltas:
                if delta == base_delta_value and base_status != 'COMPLETED':
                    # Current delta still in progress
                    processed_jobs.append({
                        "business_loaddt": biz_str,
                        "status": base_status,
                        "restart_point": base_restart_point,
                        "total_records": base_total_records,
                        "task_exec_secs": base_exec_secs,
                        "extraction_time": base_extraction_time,
                        "delta_column_value": delta,
                        "load_type": 'historic'
                    })
                else:
                    # New delta
                    processed_jobs.append({
                        "business_loaddt": biz_str,
                        "status": "NOT_STARTED",
                        "restart_point": 0,
                        "total_records": base_total_records,
                        "task_exec_secs": base_exec_secs,
                        "extraction_time": base_extraction_time,
                        "delta_column_value": delta,
                        "load_type": 'historic'
                    })

            return processed_jobs

    except Exception as e:
        logger.error("CRITICAL: Failed to get historic load status: %s", e, exc_info=True)
        raise RuntimeError(f"Historic load status query failed: {e}")


def get_delta_load_status(config_audit: Dict[str, Any], source_table: str,
                         current_business_loaddt: str) -> List[Dict[str, Any]]:
    """Get delta load status - only return incomplete jobs."""
    query = f"""
        SELECT
            business_loaddt,
            NVL(status, 'NOT_STARTED') AS status,
            NVL(total_records, 0) AS total_records,
            delta_column_value,
            NVL(load_type, 'delta') AS load_type
        FROM {config_audit["schema"]}.{config_audit["audit_table"]}
        WHERE business_loaddt <= TO_DATE(:current_dt, :fmt)
          AND source_table = :src
          AND NVL(load_type, 'delta') = 'delta'
          AND NVL(status, 'NOT_STARTED') IN ('NOT_STARTED', 'FAILED', 'RUNNING')
        ORDER BY business_loaddt
    """

    with connect_to_oracle(config_audit["target"]) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(query, {"current_dt": current_business_loaddt, "fmt": ORACLE_DATE, "src": source_table})
                rows = cur.fetchall()
                processed: List[Dict[str, Any]] = []

                for business_dt, status, total_records, delta_val, load_type in rows:
                    if hasattr(business_dt, "strftime"):
                        biz_str = business_dt.strftime("%Y-%m-%d")
                    else:
                        biz_str = datetime.strptime(business_dt, '%m-%b-%d').strftime("%Y-%m-%d") if hasattr(business_dt, "strftime") else str(business_dt)
                    processed.append({
                        "business_loaddt": biz_str,
                        "status": status,
                        "total_records": int(total_records or 0),
                        "delta_column_value": delta_val,
                        "load_type": load_type
                    })

                if not processed:
                    # Check if COMPLETED record exists
                    completed_check_query = f"""
                        SELECT COUNT(*) FROM {config_audit["schema"]}.{config_audit["audit_table"]}
                        WHERE business_loaddt = TO_DATE(:current_dt, :fmt)
                          AND source_table = :src
                          AND NVL(load_type, 'delta') = 'delta'
                          AND delta_column_value IS NULL
                          AND status = 'COMPLETED'
                    """
                    cur.execute(completed_check_query, {"current_dt": current_business_loaddt, "fmt": ORACLE_DATE, "src": source_table})
                    completed_exists = cur.fetchone()[0] > 0

                    if not completed_exists:
                        logger.info("No audit record found for %s on %s. Creating new entry.", source_table, current_business_loaddt)
                        processed.append({
                            "business_loaddt": current_business_loaddt,
                            "status": "NOT_STARTED",
                            "restart_point": 0,
                            "total_records": 0,
                            "delta_column_value": None,
                            "load_type": 'delta'
                        })
                    else:
                        logger.info("Job for %s on %s already COMPLETED. Skipping.", source_table, current_business_loaddt)

                return processed
            except Exception as e:
                logger.error("CRITICAL: Failed to get delta load status: %s", e, exc_info=True)
                raise RuntimeError(f"Delta load status query failed: {e}")
            

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def get_load_status_and_dates(config_audit: Dict[str, Any], source_table: str,
                             current_business_loaddt: str, load_type: str = 'delta',
                             delta_column: Optional[str] = None,
                             delta_column_type: Optional[Dict[str, Any]] = None,
                             delta_column_format: Optional[Dict[str, Any]] = None,
                             oracle_config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Returns list of incomplete jobs only - skips COMPLETED jobs."""
    if load_type == 'historic': # and delta_column and oracle_config:
        logger.info(f"Inside Historic get_historic_load_status method, oracle_config {oracle_config}") 
        return get_historic_load_status(config_audit, source_table, current_business_loaddt, delta_column,delta_column_type,delta_column_format, oracle_config)
    else:
        return get_delta_load_status(config_audit, source_table, current_business_loaddt)