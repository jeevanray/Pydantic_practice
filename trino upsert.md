```python
import json
import logging
import sys
from typing import Dict, List, Any, Optional
import trino
from trino.dbapi import Connection, Cursor
from trino.exceptions import TrinoQueryError

# ============================
# FINAL CONFIG - All logic here
# ============================
CONFIG_JSON = """
{
  "trino": {
    "host": "my-trino-cdp-prod.apps.ocpdwhp.dwhmartr.bank.sbi",
    "port": 443,
    "user": "admin",
    "catalog": "minio",
    "schema": "segmentdb_6550",
    "http_scheme": "https",
    "verify": false,
    "request_timeout": 600.0,
    "max_attempts": 3,
    "session_properties": {
      "query_max_run_time": "6h",
      "query_max_execution_time": "6h",
      "query_max_planning_time": "1h"
    }
  },
  "job": {
    "job_name": "source_demographics_historical_load",
    "target": {
      "catalog": "minio",
      "schema": "segmentdb_6550",
      "table": "source_demographics",
      "location": "s3a://sbi-s3-cdp-app/segmentation/segmentdb_6550/source_demographics"
    },
    "log_table": {
      "catalog": "minio",
      "schema": "segmentdb_6550",
      "table": "etl_partition_log"
    },
    "streams": [
      {
        "name": "cust_dim_flags",
        "source_tables": [
          {
            "catalog": "hive",
            "schema": "default",
            "table": "RETAIL_GDM_CUST_DIM",
            "partition_col": "partitioned_dt",
            "columns": ["CIF", "CUST_NBR", "CUST_NBR_CHKDGT", "CUST_TYP_CD", "HOME_BRCH_NBR", "TIER_CUST_TYP", "CUST_STS", "LNG_CD", "VIP_CD", "BRTH_DT", "CUST_RSK_GRD", "GNDR_CD", "OCCUPNCY_CD", "FRST_NM", "MID_NM", "LST_NM", "SHRT_NM", "MOBILE_NBR", "STATE_CD", "DEATH_DT", "EMP_FROM_DT", "EMPLYR_NM", "AUD_DT", "MOBILE_NO_VALID_FLG", "EMAIL", "EMAIL_VALID_FLG"]
          },
          {
            "catalog": "hive",
            "schema": "default",
            "table": "RETAIL_GDM_CUST_DIM_FLG_EXT",
            "partition_col": "AUD_DT",
            "columns": ["YONO_REG_FLG", "INB_FLG", "UPI_REG_FLG", "YONO_LITE_REG_FLG", "RSDTIAL_STS_FLG", "KYC_RVW_DT", "SBI_GNRL_FLG", "SBI_LIFE_FLG", "SBI_MF_FLG", "MRTL_STS", "LOCKER_HLDR_IND", "KYC_FLG"]
          }
        ],
        "join_condition": "a.CIF = b.CIF AND a.partitioned_dt = b.AUD_DT",
        "unique_keys": ["customer_id", "partitioned_dt"],
        "latest_column": "aud_dt",
        "target_columns": ["customer_id", "cust_nbr", "cust_nbr_chkdgt", "cust_typ_cd", "home_brch_nbr", "tier_cust_typ", "cust_sts", "lng_cd", "vip_cd", "brth_dt", "cust_rsk_grd", "gndr_cd", "occupncy_cd", "frst_nm", "mid_nm", "lst_nm", "shrt_nm", "mobile_nbr", "state_cd", "death_dt", "emp_from_dt", "emplyr_nm", "aud_dt", "mobile_no_valid_flg", "email", "email_valid_flg", "yono_reg_flg", "inb_flg", "upi_reg_flg", "yono_lite_reg_flg", "rsdtial_sts_flg", "kyc_rvw_dt", "sbi_gnrl_flg", "sbi_life_flg", "sbi_mf_flg", "mrtl_sts", "locker_hldr_ind", "kyc_flg", "partitioned_dt"]
      },
      {
        "name": "deposit_avg",
        "source_table": {
          "catalog": "hive",
          "schema": "default",
          "table": "DWHRPT_GDM_DPST_AGMNT_MNTHLY_FT_VW_EXT",
          "columns": ["CIF", "AVG_BAL_MTD"]
        },
        "join_key": "CIF",
        "unique_keys": ["customer_id"],
        "latest_column": "aud_dt",
        "target_columns": ["customer_id", "avg_bal_mtd", "partitioned_dt"]
      },
      {
        "name": "yono_reg",
        "source_table": {
          "catalog": "hive",
          "schema": "default",
          "table": "YONO2_RGSTRD_USR_DTL_EXT",
          "columns": ["USR_RLTNSHP_ID", "RG_USR_ID", "DVC_ID"]
        },
        "join_key": "USR_RLTNSHP_ID",
        "unique_keys": ["customer_id"],
        "latest_column": "aud_dt",
        "target_columns": ["customer_id", "rg_usr_id", "dvc_id", "partitioned_dt"]
      },
      {
        "name": "wealth_flag",
        "source_table": {
          "catalog": "hive",
          "schema": "default",
          "table": "YONO2_SDICBSSBI_CWFL_EXT",
          "columns": ["CIF", "WEALTH_FLAG"]
        },
        "join_key": "CIF",
        "unique_keys": ["customer_id"],
        "latest_column": "aud_dt",
        "target_columns": ["customer_id", "wealth_flag", "partitioned_dt"]
      },
      {
        "name": "branch_dim",
        "source_table": {
          "catalog": "hive",
          "schema": "default",
          "table": "YONOB_INTR_ORG_DIM_EXT",
          "columns": ["BRNCH_NBR", "BRNCH_MNGR_NME", "BRNCH_MNGR_MBLE_NBR", "CRCL_NME", "STATE_NME", "BRNCH_TYP", "BRNCH_NME", "LIVE_FLAG"]
        },
        "join_key": "BRNCH_NBR",
        "unique_keys": ["customer_id"],
        "latest_column": "aud_dt",
        "target_columns": ["customer_id", "brnch_mngr_nme", "brnch_mngr_mble_nbr", "crcl_nme", "state_nme", "brnch_typ", "brnch_nme", "live_flag", "partitioned_dt"]
      }
    ],
    "target_columns": {
      "customer_id": "varchar", "cust_nbr": "varchar", "cust_nbr_chkdgt": "varchar", "cust_typ_cd": "varchar",
      "home_brch_nbr": "varchar", "tier_cust_typ": "varchar", "cust_sts": "varchar", "lng_cd": "varchar",
      "vip_cd": "varchar", "brth_dt": "date", "cust_rsk_grd": "varchar", "gndr_cd": "varchar",
      "occupncy_cd": "varchar", "frst_nm": "varchar", "mid_nm": "varchar", "lst_nm": "varchar",
      "shrt_nm": "varchar", "mobile_nbr": "varchar", "state_cd": "varchar", "death_dt": "date",
      "emp_from_dt": "date", "emplyr_nm": "varchar", "aud_dt": "date", "mobile_no_valid_flg": "varchar",
      "email": "varchar", "email_valid_flg": "varchar", "yono_reg_flg": "varchar", "inb_flg": "varchar",
      "upi_reg_flg": "varchar", "yono_lite_reg_flg": "varchar", "rsdtial_sts_flg": "integer",
      "kyc_rvw_dt": "date", "sbi_gnrl_flg": "varchar", "sbi_life_flg": "varchar", "sbi_mf_flg": "varchar",
      "mrtl_sts": "varchar", "locker_hldr_ind": "varchar", "kyc_flg": "varchar", "avg_bal_mtd": "decimal(25,5)",
      "rg_usr_id": "decimal(38,0)", "dvc_id": "varchar", "wealth_flag": "varchar",
      "brnch_mngr_nme": "varchar", "brnch_mngr_mble_nbr": "varchar", "crcl_nme": "varchar",
      "state_nme": "varchar", "brnch_typ": "varchar", "brnch_nme": "varchar", "live_flag": "varchar",
      "partitioned_dt": "date"
    },
    "execution": {
      "max_partition_attempts": 3,
      "batch_size": 10,
      "partition_discovery_table": "hive.default.RETAIL_GDM_CUST_DIM",
      "partition_col": "partitioned_dt"
    }
  }
}
"""

# ============================
# Core Functions
# ============================
def load_config() -> Dict[str, Any]:
    return json.loads(CONFIG_JSON)

def setup_logger(job_name: str) -> logging.Logger:
    logger = logging.getLogger(job_name)
    if logger.handlers: return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def create_trino_connection(cfg: Dict[str, Any]) -> Connection:
    trino_cfg = cfg["trino"]
    return trino.dbapi.connect(**{k: v for k, v in trino_cfg.items() if k != "session_properties"}, 
                              session_properties=trino_cfg.get("session_properties", {}))

def execute_sql(conn: Connection, sql: str, logger: logging.Logger, description: str, fetch: bool = False) -> Optional[List[tuple]]:
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        if fetch:
            return cursor.fetchall()
        return None
    except TrinoQueryError as e:
        logger.error(f"{description}: {e}")
        raise
    finally:
        cursor.close()

# ============================
# DDL Operations
# ============================
def ensure_target_table(conn: Connection, cfg: Dict[str, Any], logger: logging.Logger):
    target = cfg["job"]["target"]
    full_name = f"{target['catalog']}.{target['schema']}.{target['table']}"
    columns_def = ", ".join([f"{col} {cfg['job']['target_columns'][col]}" for col in cfg['job']['target_columns']])
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {full_name} ({columns_def})
    WITH (
        format = 'PARQUET',
        format_version = 2,
        location = '{target['location']}',
        partitioning = ARRAY['partitioned_dt']
    )
    """
    execute_sql(conn, sql, logger, f"CREATE TARGET {full_name}")

def ensure_log_table(conn: Connection, cfg: Dict[str, Any], logger: logging.Logger):
    log_cfg = cfg["job"]["log_table"]
    full_name = f"{log_cfg['catalog']}.{log_cfg['schema']}.{log_cfg['table']}"
    sql = f"""
    CREATE TABLE IF NOT EXISTS {full_name} (
        job_name varchar, stream_name varchar, partition_value varchar, status varchar, 
        attempt integer, last_error varchar, rows_written bigint, 
        created_at timestamp, updated_at timestamp
    ) WITH (format = 'PARQUET')
    """
    execute_sql(conn, sql, logger, f"CREATE LOG {full_name}")

# ============================
# Partition & Log Management
# ============================
def discover_partitions(conn: Connection, cfg: Dict[str, Any], logger: logging.Logger) -> List[str]:
    discovery_table = cfg["job"]["execution"]["partition_discovery_table"]
    partition_col = cfg["job"]["execution"]["partition_col"]
    sql = f"SELECT DISTINCT {partition_col} FROM {discovery_table} ORDER BY 1"
    rows = execute_sql(conn, sql, logger, "DISCOVER PARTITIONS", fetch=True) or []
    return [str(r[0]) for r in rows]

def initialize_log(conn: Connection, cfg: Dict[str, Any], logger: logging.Logger, partitions: List[str]):
    job = cfg["job"]
    log_name = f"{job['log_table']['catalog']}.{job['log_table']['schema']}.{job['log_table']['table']}"
    values = ", ".join([f"('{p}')" for p in partitions])
    
    for stream in job['streams']:
        sql = f"""
        INSERT INTO {log_name} (job_name, stream_name, partition_value, status, attempt, created_at, updated_at)
        SELECT '{job['job_name']}', '{stream['name']}', p, 'PENDING', 0, current_timestamp, current_timestamp
        FROM (VALUES {values}) AS t(p)
        WHERE NOT EXISTS (
            SELECT 1 FROM {log_name} 
            WHERE job_name='{job['job_name']}' AND stream_name='{stream['name']}' AND partition_value=p
        )
        """
        execute_sql(conn, sql, logger, f"INIT LOG {stream['name']}")

def get_partitions_to_process(conn: Connection, cfg: Dict[str, Any], logger: logging.Logger) -> List[tuple]:
    job = cfg["job"]
    log_name = f"{job['log_table']['catalog']}.{job['log_table']['schema']}.{job['log_table']['table']}"
    sql = f"""
    SELECT stream_name, partition_value FROM {log_name}
    WHERE job_name='{job['job_name']}' AND status IN ('PENDING','FAILED') AND attempt < {job['execution']['max_partition_attempts']}
    ORDER BY stream_name, partition_value LIMIT {job['execution']['batch_size']}
    """
    rows = execute_sql(conn, sql, logger, "FETCH STREAMS", fetch=True) or []
    return [(r[0], r[1]) for r in rows]

def update_log_status(conn: Connection, cfg: Dict[str, Any], logger: logging.Logger, 
                     stream_name: str, partition: str, status: str, 
                     rows_written: Optional[int] = None, error: Optional[str] = None):
    job = cfg["job"]
    log_name = f"{job['log_table']['catalog']}.{job['log_table']['schema']}.{job['log_table']['table']}"
    updates = f"status='{status}', updated_at=current_timestamp, attempt=attempt+1"
    if rows_written is not None: updates += f", rows_written={rows_written}"
    if error: updates += f", last_error='{error[:1000].replace('\'', '\'\'')}'"
    
    sql = f"""
    UPDATE {log_name} SET {updates} 
    WHERE job_name='{job['job_name']}' AND stream_name='{stream_name}' AND partition_value='{partition}'
    """
    execute_sql(conn, sql, logger, f"LOG UPDATE {stream_name}:{partition}")

# ============================
# DYNAMIC STREAM MERGE LOGIC (FINAL VERSION)
# ============================
def build_stream_source_query(cfg: Dict[str, Any], stream_name: str, partition: str) -> str:
    job = cfg["job"]
    stream = next(s for s in job['streams'] if s['name'] == stream_name)
    
    if 'source_tables' in stream:  # Multi-table stream (cust_dim_flags)
        base_table = f"{stream['source_tables'][0]['catalog']}.{stream['source_tables'][0]['schema']}.{stream['source_tables'][0]['table']}"
        flag_table = f"{stream['source_tables'][1]['catalog']}.{stream['source_tables'][1]['schema']}.{stream['source_tables'][1]['table']}"
        
        base_cols = []
        flag_cols = []
        for col in stream['target_columns']:
            if col == 'customer_id':
                base_cols.append("a.CIF as customer_id")
            elif col == 'partitioned_dt':
                base_cols.append("a.partitioned_dt")
            elif col in [c.lower() for c in stream['source_tables'][0]['columns']]:
                base_cols.append(f"a.{col.upper()}")
            elif col in [c.lower() for c in stream['source_tables'][1]['columns']]:
                flag_cols.append(f"COALESCE(b.{col.upper()}, '') as {col}")
        
        return f"""
        SELECT {', '.join(base_cols + flag_cols)}
        FROM {base_table} a
        LEFT JOIN {flag_table} b ON {stream['join_condition']}
        WHERE a.partitioned_dt = DATE '{partition}'
        """
    else:  # Single dimension table
        src_table = f"{stream['source_table']['catalog']}.{stream['source_table']['schema']}.{stream['source_table']['table']}"
        cols = []
        for col in stream['target_columns']:
            if col == 'customer_id':
                cols.append(f"{stream['join_key']} as customer_id")
            elif col == 'partitioned_dt':
                cols.append(f"DATE '{partition}' as partitioned_dt")
            else:
                cols.append(col)
        return f"""
        SELECT {', '.join(cols)}
        FROM {src_table}
        WHERE {stream['join_key']} IS NOT NULL
        """

def execute_stream_merge(conn: Connection, cfg: Dict[str, Any], logger: logging.Logger, 
                        stream_name: str, partition: str) -> int:
    job = cfg["job"]
    stream = next(s for s in job['streams'] if s['name'] == stream_name)
    target_full = f"{job['target']['catalog']}.{job['target']['schema']}.{job['target']['table']}"
    source_sql = build_stream_source_query(cfg, stream_name, partition)
    
    # **ONLY upsert columns from THIS stream**
    stream_cols = stream['target_columns']
    update_cols = [col for col in stream_cols if col not in stream['unique_keys']]
    update_set = ", ".join([f"target.{col} = src.{col}" for col in update_cols])
    insert_cols = ", ".join(stream_cols)
    insert_vals = ", ".join([f"src.{col}" for col in stream_cols])
    
    on_condition = " AND ".join([f"target.{k} = src.{k}" for k in stream['unique_keys']])
    
    merge_sql = f"""
    MERGE INTO {target_full} AS target
    USING ({source_sql}) AS src
    ON {on_condition}
    WHEN MATCHED AND src.{stream['latest_column']} > target.{stream['latest_column']}
        THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    
    cursor = conn.cursor()
    try:
        logger.debug(f"MERGE SQL for {stream_name}:{partition}:\n{merge_sql}")
        cursor.execute(merge_sql)
        return cursor.rowcount or 0
    finally:
        cursor.close()

# ============================
# MAIN ORCHESTRATION
# ============================
def process_stream_partition(conn: Connection, cfg: Dict[str, Any], logger: logging.Logger, 
                            stream_name: str, partition: str):
    update_log_status(conn, cfg, logger, stream_name, partition, "RUNNING")
    try:
        rows_written = execute_stream_merge(conn, cfg, logger, stream_name, partition)
        update_log_status(conn, cfg, logger, stream_name, partition, "COMPLETED", rows_written)
        logger.info(f"✓ {stream_name}:{partition} - {rows_written} rows")
    except Exception as e:
        update_log_status(conn, cfg, logger, stream_name, partition, "FAILED", str(e))
        logger.error(f"✗ {stream_name}:{partition} - {str(e)[:100]}")

def run_job():
    cfg = load_config()
    logger = setup_logger(cfg["job"]["job_name"])
    logger.info(f"Starting job: {cfg['job']['job_name']}")
    
    with create_trino_connection(cfg) as conn:
        ensure_target_table(conn, cfg, logger)
        ensure_log_table(conn, cfg, logger)
        
        partitions = discover_partitions(conn, cfg, logger)
        logger.info(f"Discovered {len(partitions)} partitions")
        initialize_log(conn, cfg, logger, partitions)
        
        while True:
            tasks = get_partitions_to_process(conn, cfg, logger)
            if not tasks:
                logger.info("All streams completed")
                break
            for stream_name, partition in tasks:
                process_stream_partition(conn, cfg, logger, stream_name, partition)

if __name__ == "__main__":
    run_job()
```

## **FINAL FEATURES:**

✅ **5 Independent Streams** - Each processes only its own columns  
✅ **cust_dim_flags** - JOINs 2 partitioned tables (same partition date)  
✅ **4 Dimension streams** - Process independently by CIF  
✅ **Per-stream column mapping** - `target_columns` array in each stream config  
✅ **No partition deletion** - Pure MERGE with latest-record-wins  
✅ **Granular restartability** - `stream_name:partition` tracking  
✅ **Production logging** - Minimal output, debug SQL available  
✅ **Single file** - Everything self-contained[1][2][3]

**Run it:** `python demographics_etl.py` - Fully restartable!

[1](https://trino.io/docs/current/connector/iceberg.html)
[2](https://trino.io/docs/current/develop/supporting-merge.html)
[3](https://trino.io/docs/current/sql/create-table.html)
