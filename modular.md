Below is a clean, modular Python code refactor and a pattern for Airflow **dynamic DAG generation** based on your requirements and the best practices recommended by the Airflow community.[1][2][3]

***

## 1. ETL Core Logic: `oracle_minio_etl.py`

**This module contains all extract-load functions and helpers.**
```python
# oracle_minio_etl.py

import logging
from datetime import datetime
from typing import Dict, Any, Optional, Sequence
import pandas as pd
import pytz

# import your custom dependencies
# from minio_handler import MinioHandler

IST = pytz.timezone("Asia/Kolkata")
DATETIMEFORMAT = "%Y-%m-%d %H:%M:%S"

logger = logging.getLogger("Minio_framework")

def connect_to_oracle(oracle_conf: Dict[str, Any]):
    # add your connect logic
    pass

def close_connection(cursor, connection):
    # safely close cursor and connection
    pass

def create_audit_table_if_not_exists(config_audit: Dict[str, Any]):
    # table DDL creation logic
    pass

def prepare_auditing() -> Dict[str, Any]:
    # returns base audit dict
    pass

def initialize_restart_audit_log(config_audit, audit_log, aud_dt, delta_column_value=None):
    # logic to restart from previous incomplete
    pass

def update_audit_record_strict(config_audit, audit_data, max_attempts=5, wait_seconds=3):
    # update with retry and error handling
    pass

def generate_object_path(base_path, business_loaddt, load_type, delta_column_value=None, sub_folder=None):
    load_dt = datetime.strptime(business_loaddt, "%Y-%m-%d")
    date_folder = load_dt.strftime("%d%m%Y")
    if load_type == 'historic' and delta_column_value:
        clean_delta = str(delta_column_value).replace("-", "")
        return f"{base_path}/history/{clean_delta}"
    return f"{base_path}/delta/{date_folder}"

def oracle_to_minio_parquet(
    oracle_config: Dict[str, Any],
    minio_config: Dict[str, Any],
    config_audit: Dict[str, Any],
    table_name: str,
    base_object_path: str,
    business_loaddt: str,
    *,
    chunk_size: int = 100_000,
    compression: str = "snappy",
    order_by: Optional[str] = None,
    restart_point: int = 0,
    select_columns: Optional[Sequence[str]] = None,
    mapping_column: Optional[str] = None,
    delta_columns: Optional[Sequence[str]] = None,
    load_type: str = 'delta',
    delta_column: Optional[str] = None,
    delta_column_value: Optional[str] = None,
    sub_folder: Optional[str] = None,
    where_clause: Optional[str] = None,
) -> None:
    # main ETL logic as in your script
    pass

def process_oracle_to_minio_with_dependencies(
    oracle_config,
    minio_config,
    config_audit,
    table_name,
    base_object_path,
    current_business_loaddt,
    *,
    chunk_size=100_000,
    compression="snappy",
    order_by=None,
    load_type='delta',
    delta_column=None,
    sub_folder=None,
):
    # looping logic for jobs as in your script
    pass
```
***

## 2. Driver/Workflow: `etl_driver.py`

**This is your clean flow and can be used for CLI invocation or Airflow task.**
```python
# etl_driver.py

import logging
from datetime import datetime
import yaml
from cdp_diapi_adapter import get_system_config
from oracle_minio_etl import process_oracle_to_minio_with_dependencies

def process_from_config(config, conn_config, current_business_loaddt):
    oracle_config = conn_config.get("target", {})
    minio_config = conn_config.get("minio", {})
    logger = logging.getLogger("Minio_framework")

    audit_config = {
        "target": oracle_config,
        "schema": config.get("schema", "uds"),
        "audit_table": config.get("audit_config", {}).get("audit_table", "AIRFLOW_CDP_DIAPI_RUN_LOG"),
    }

    for obj_config in config.get("objects", []):
        if not obj_config.get("isactive", True):
            logger.info(f"Skipping inactive object: {obj_config.get('db_table')}")
            continue
        table_name = f"{obj_config.get('schema', config.get('schema'))}.{obj_config['db_table']}"
        output_path = obj_config.get("output_path", config.get("output_path"))
        load_type = obj_config.get("load_type", "delta")
        chunk_size = obj_config.get("chunksize", config.get("chunksize", 100_000))
        order_by = obj_config.get("ORDER_BY")
        delta_column = obj_config.get("delta_column") if load_type == 'historic' else None
        sub_folder = obj_config.get("sub_folder") if load_type == 'historic' else None

        process_oracle_to_minio_with_dependencies(
            oracle_config=oracle_config,
            minio_config=minio_config,
            config_audit=audit_config,
            table_name=table_name,
            base_object_path=output_path,
            current_business_loaddt=current_business_loaddt,
            chunk_size=chunk_size,
            order_by=order_by,
            load_type=load_type,
            delta_column=delta_column,
            sub_folder=sub_folder,
        )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    try:
        conn_config = get_system_config()
        config_yaml = "etl_configs/db2_to_uds_config.yml"
        with open(config_yaml, 'r', encoding="utf-8") as f:
            config_data = yaml.safe_load(f)["uds_to_minio"]
        current_date = datetime.now().strftime("%Y-%m-%d")
        process_from_config(config_data, conn_config, current_date)
    except Exception as e:
        logging.error(f"Main execution failed: {e}")
        raise
```
***

## 3. Dynamic Airflow DAG: `dags/etl_dynamic.py`

**Auto-generate a DAG per table in config using the imported ETL logic.**
```python
# dags/etl_dynamic.py

import os
import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from cdp_diapi_adapter import get_system_config
from etl_driver import process_from_config

config_yaml = os.environ.get("ETL_CONFIG_PATH", "/path/to/etl_configs/db2_to_uds_config.yml")
with open(config_yaml, 'r', encoding="utf-8") as f:
    config_data = yaml.safe_load(f)["uds_to_minio"]

conn_config = get_system_config()
current_business_loaddt = datetime.now().strftime("%Y-%m-%d")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),  # set appropriately
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

for obj in config_data.get("objects", []):
    if not obj.get("isactive", True):
        continue
    dag_id = f"oracle_minio_etl_{obj['db_table']}"
    table_config = {**config_data, "objects": [obj]}  # Single-table config for driver
    dag = DAG(dag_id, default_args=default_args, schedule_interval='@daily', catchup=False)

    def _etl_task(**kwargs):
        process_from_config(table_config, conn_config, current_business_loaddt)

    with dag:
        etl_task = PythonOperator(
            task_id=f"etl_{obj['db_table']}",
            python_callable=_etl_task,
            provide_context=True,
        )

    globals()[dag_id] = dag
```
- This creates a DAG per active table. Each DAG executes the ETL logic, pulling config per table.[4][2][1]

***

## Notes
- Place `oracle_minio_etl.py` and `etl_driver.py` in a module folder accessible to your Airflow/dags directory.
- Only import what is needed in each module to maintain separation of concerns.[5]
- Any logic tightly coupled to ETL (DB, audit, MinIO, helpers) stays in `oracle_minio_etl.py`; all orchestration is kept in `etl_driver.py`.
- You may further split `oracle_minio_etl.py` if the DB/audit or MinIO logic grows large.

## References
- [Dynamic DAG generation official Airflow Docs][4]
- [Hevo Data Airflow dynamic DAG tutorial][1]
- [Astronomer - Dynamically generate DAGs][2]
- [Airflow Best Practices][3]

***

This structure will make your ETL code clean to **import and reuse in both Airflow DAGs and standalone runs**. Each new table addition only requires updating the config file—no DAG file changes.

[1](https://hevodata.com/learn/airflow-dynamic-dags/)
[2](https://www.astronomer.io/docs/learn/dynamically-generating-dags/)
[3](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
[4](https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dag-generation.html)
[5](https://www.anycode.ai/split-code-tutorial/how-to-ensure-that-splitting-code-across-files-maintains-a-clear-separation-of-concerns)
[6](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/90742002/41f7202b-e1a5-44c9-a0e8-fe29c8c356b4/paste.txt)
[7](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
[8](https://www.youtube.com/watch?v=b-ZSnOZZdio)
[9](https://instrumentationtools.com/plc-best-practices-split-plc-code-into-modules/)
[10](https://airflow.apache.org/docs/apache-airflow/2.3.0/_api/airflow/models/dag/index.html)
[11](https://hevodata.com/learn/python-dag-airflow/)
[12](https://hevodata.com/learn/airflow-dags/)
[13](https://www.astronomer.io/events/webinars/airflow-dynamic-dags-video/)
[14](https://www.timmitchell.net/post/2016/07/11/etl-modularity/)
[15](https://airflow.apache.org/docs/apache-airflow/1.10.9/concepts.html)
[16](https://blogs.halodoc.io/dynamic-dag-generation-in-airflow-best-practices-and-use-cases/)
[17](https://stackoverflow.com/questions/63696464/advice-on-structuring-etl-application-code)
[18](https://stackoverflow.com/questions/55959381/how-to-efficiently-make-airflow-dag-definitions-database-driven/55960330)
[19](https://www.youtube.com/watch?v=BkhwK4iAdmc)
[20](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
[21](https://www.blef.fr/airflow-dynamic-dags/)
