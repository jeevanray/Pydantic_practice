Based on your working code, I'll provide a comprehensive modular refactoring that makes it reusable for Airflow orchestration. Here's the complete restructured solution:

## Directory Structure

```
project/
├── src/
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   └── exceptions.py
│   ├── database/
│   │   ├── __init__.py
│   │   ├── oracle_client.py
│   │   └── schema_manager.py
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── minio_client.py
│   │   └── arrow_processor.py
│   ├── audit/
│   │   ├── __init__.py
│   │   └── audit_manager.py
│   ├── processors/
│   │   ├── __init__.py
│   │   └── data_extractor.py
│   └── utils/
│       ├── __init__.py
│       └── helpers.py
└── orchestration/
    ├── airflow_dags/
    │   ├── oracle_to_minio_dag.py
    │   └── config_driven_dag.py
    └── tasks/
        ├── __init__.py
        └── extraction_tasks.py
```

## Core Configuration Module

**src/core/config.py**
```python
"""Core configuration and constants module."""
import logging
import pytz
from typing import Dict, Any

# Constants
DATETIMEFORMAT = "%Y-%m-%d %H:%M:%S"
ORACLE_DATE = "YYYY-MM-DD"
IST = pytz.timezone("Asia/Kolkata")

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Setup standardized logger."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - PID:%(process)d - TID:%(thread)d - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def validate_config(config: Dict[str, Any], required_keys: list) -> None:
    """Validate configuration has required keys."""
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Missing required configuration keys: {missing_keys}")
```

## Database Layer

**src/database/oracle_client.py**
```python
"""Oracle database connection and operations."""
import oracledb
from typing import Dict, Any, List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from contextlib import contextmanager

from ..core.config import setup_logger
from ..core.exceptions import DatabaseConnectionError

logger = setup_logger(__name__)

class OracleClient:
    """Oracle database client with connection management."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def create_connection(self) -> oracledb.Connection:
        """Create Oracle database connection with retry logic."""
        logger.info("Connecting to Oracle DB")
        try:
            conn = oracledb.connect(
                user=self.config.get("username", self.config.get("user", "")),
                password=self.config["password"],
                dsn=self.config["dsn"],
            )
            logger.info("Oracle connection successful")
            return conn
        except Exception as e:
            logger.error("Failed to connect to Oracle database: %s", e)
            raise DatabaseConnectionError(f"Oracle connection failed: {e}")
    
    @contextmanager
    def connection(self):
        """Context manager for Oracle connections."""
        conn = None
        try:
            conn = self.create_connection()
            yield conn
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning("Error closing Oracle connection: %s", e)
    
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[tuple]:
        """Execute a query and return results."""
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or {})
                return cur.fetchall()
    
    def get_streaming_cursor(self, query: str, params: Dict[str, Any] = None, array_size: int = 10000):
        """Get a streaming cursor for large result sets."""
        conn = self.create_connection()
        cur = conn.cursor()
        cur.arraysize = array_size
        cur.execute(query, params or {})
        return conn, cur
```

## Storage Layer

**src/storage/arrow_processor.py**
```python
"""PyArrow data processing utilities."""
import pyarrow as pa
from typing import List, Any
from oracledb import LOB
from datetime import datetime

from ..core.config import setup_logger

logger = setup_logger(__name__)

class ArrowProcessor:
    """Handles PyArrow data processing operations."""
    
    @staticmethod
    def sanitize_column_data(column_values: List[Any], field: pa.Field) -> List[Any]:
        """Sanitize a single column's values for LOBs and data type conversions."""
        sanitized_values = []
        field_type = field.type

        for value in column_values:
            if value is None:
                sanitized_values.append(None)
            elif isinstance(value, LOB):
                try:
                    lob_data = value.read()
                    if pa.types.is_string(field_type):
                        if isinstance(lob_data, bytes):
                            sanitized_values.append(lob_data.decode('utf-8', errors='ignore'))
                        else:
                            sanitized_values.append(str(lob_data) if lob_data is not None else None)
                    elif pa.types.is_binary(field_type):
                        if isinstance(lob_data, bytes):
                            sanitized_values.append(lob_data)
                        else:
                            sanitized_values.append(str(lob_data).encode('utf-8') if lob_data is not None else None)
                    else:
                        sanitized_values.append(lob_data)
                except Exception as e:
                    logger.warning(f"Failed to read LOB for column {field.name}: {e}")
                    sanitized_values.append(None)
            elif pa.types.is_date32(field_type) and isinstance(value, datetime):
                sanitized_values.append(value.date())
            else:
                sanitized_values.append(value)

        return sanitized_values
    
    @staticmethod
    def cast_table_columns_to_schema(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
        """Cast table columns to match target schema."""
        casted_table = table

        for field in target_schema:
            if field.name not in table.column_names:
                continue

            col_idx = casted_table.schema.get_field_index(field.name)
            if col_idx == -1:
                continue

            current_column = casted_table[field.name]

            if not current_column.type.equals(field.type):
                try:
                    if current_column.null_count == len(current_column):
                        null_values = [None] * len(current_column)
                        typed_null_array = pa.array(null_values, type=field.type)
                        casted_column = pa.chunked_array([typed_null_array])
                    else:
                        try:
                            casted_column = current_column.cast(field.type, safe=True)
                        except pa.ArrowInvalid:
                            casted_column = current_column.cast(field.type, safe=False)

                    casted_table = casted_table.set_column(col_idx, field.name, casted_column)
                except Exception as e:
                    logger.warning(f"Failed to cast column {field.name}: {e}")
                    continue

        return casted_table
    
    def create_arrow_table_from_rows(self, rows: List[tuple], column_names: List[str], target_schema: pa.Schema) -> pa.Table:
        """Create PyArrow table from Oracle cursor rows."""
        if not rows:
            empty_arrays = [pa.array([], type=f.type) for f in target_schema]
            return pa.Table.from_arrays(empty_arrays, names=column_names)

        columns_data = list(zip(*rows))
        arrays = []
        
        for i, (column_data, field) in enumerate(zip(columns_data, target_schema)):
            sanitized_column = self.sanitize_column_data(list(column_data), field)
            arrays.append(pa.array(sanitized_column))

        table = pa.Table.from_arrays(arrays, names=column_names)
        return self.cast_table_columns_to_schema(table, target_schema)
```

## Data Extraction Processor

**src/processors/data_extractor.py**
```python
"""Main data extraction processor."""
from typing import Dict, Any, Optional
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..core.config import setup_logger, DATETIMEFORMAT, IST
from ..database.oracle_client import OracleClient
from ..database.schema_manager import SchemaManager
from ..storage.minio_client import MinioClient
from ..storage.arrow_processor import ArrowProcessor
from ..audit.audit_manager import AuditManager
from ..utils.helpers import generate_object_path

logger = setup_logger(__name__)

class DataExtractor:
    """Main data extraction processor."""
    
    def __init__(self, oracle_config: Dict[str, Any], minio_config: Dict[str, Any], audit_config: Dict[str, Any]):
        self.oracle_client = OracleClient(oracle_config)
        self.minio_client = MinioClient(minio_config)
        self.audit_manager = AuditManager(audit_config)
        self.schema_manager = SchemaManager(oracle_config)
        self.arrow_processor = ArrowProcessor()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def extract_table_to_parquet(
        self,
        table_name: str,
        base_object_path: str,
        business_loaddt: str,
        *,
        chunk_size: int = 100_000,
        compression: str = "snappy",
        order_by: Optional[str] = None,
        restart_point: int = 0,
        load_type: str = 'delta',
        delta_column: Optional[str] = None,
        delta_column_value: Optional[str] = None,
        sub_folder: Optional[str] = None,
    ) -> None:
        """Extract data from Oracle table to MinIO parquet files."""
        
        # Validation
        self._validate_extraction_params(table_name, base_object_path, business_loaddt, load_type, delta_column_value)
        
        extraction_start = datetime.now(IST)
        effective_object_path = generate_object_path(
            base_object_path, business_loaddt, load_type, delta_column_value, sub_folder
        )
        
        # Initialize audit
        audit_log = self.audit_manager.prepare_audit_log(
            table_name, business_loaddt, delta_column_value, load_type, 
            extraction_start, effective_object_path
        )
        
        # Check if already completed
        if self.audit_manager.is_job_completed(audit_log):
            logger.info(f"Job already completed for {table_name}. Skipping.")
            return
        
        # Get schema
        oracle_schema, _ = self.schema_manager.get_table_schema(table_name)
        
        # Build query
        where_clause = self._build_where_clause(load_type, delta_column, delta_column_value)
        order_clause = f" ORDER BY {order_by}" if order_by else ""
        select_sql = f"SELECT * FROM {table_name}{where_clause}{order_clause}"
        
        # Process data in chunks
        self._process_data_chunks(
            select_sql, oracle_schema, effective_object_path, table_name,
            chunk_size, compression, restart_point, audit_log, 
            load_type, delta_column_value, extraction_start
        )
    
    def _validate_extraction_params(self, table_name: str, base_object_path: str, 
                                   business_loaddt: str, load_type: str, delta_column_value: Optional[str]):
        """Validate extraction parameters."""
        if not table_name or not table_name.strip():
            raise ValueError("table_name is required and cannot be empty")
        if not base_object_path:
            raise ValueError("base_object_path is required")
        
        try:
            datetime.strptime(business_loaddt, "%Y-%m-%d")
        except ValueError:
            raise ValueError("business_loaddt must be in YYYY-MM-DD format")
        
        if load_type == 'historic' and not delta_column_value:
            raise ValueError("delta_column_value is required for historic load type")
    
    def _build_where_clause(self, load_type: str, delta_column: Optional[str], 
                           delta_column_value: Optional[str]) -> str:
        """Build WHERE clause for the query."""
        if load_type == 'historic' and delta_column and delta_column_value:
            return f" WHERE {delta_column} = TO_DATE('{delta_column_value}', 'YYYY-MM-DD')"
        return ""
    
    def _process_data_chunks(self, select_sql: str, oracle_schema, effective_object_path: str,
                           table_name: str, chunk_size: int, compression: str, 
                           restart_point: int, audit_log: Dict[str, Any], 
                           load_type: str, delta_column_value: Optional[str], 
                           extraction_start: datetime):
        """Process data in chunks and upload to MinIO."""
        
        conn, cur = self.oracle_client.get_streaming_cursor(select_sql, array_size=max(10_000, min(chunk_size, 100_000)))
        
        try:
            # Skip to restart point
            for _ in range(restart_point):
                skipped = cur.fetchmany(chunk_size)
                if not skipped:
                    break
            
            chunk_index = restart_point
            column_names = [field.name for field in oracle_schema]
            total_records = int(audit_log.get("total_records", 0))
            
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                
                # Create Arrow table
                table_chunk = self.arrow_processor.create_arrow_table_from_rows(
                    rows, column_names, oracle_schema
                )
                
                # Generate object name
                object_name = self._generate_chunk_object_name(
                    effective_object_path, table_name, load_type, 
                    delta_column_value, chunk_index
                )
                
                # Upload to MinIO
                self.minio_client.upload_arrow_table_as_parquet(
                    table_chunk, object_name, compression
                )
                
                # Update audit
                total_records += table_chunk.num_rows
                self._update_chunk_audit(
                    audit_log, chunk_index, total_records, 
                    extraction_start, effective_object_path
                )
                
                chunk_index += 1
            
            # Final audit update
            self._finalize_audit(audit_log, extraction_start)
            
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    def _generate_chunk_object_name(self, effective_object_path: str, table_name: str,
                                   load_type: str, delta_column_value: Optional[str], 
                                   chunk_index: int) -> str:
        """Generate object name for chunk."""
        clean_delta = str(delta_column_value).replace("-", "") if delta_column_value else ""
        
        if load_type == 'historic':
            return f"{effective_object_path}/{table_name.replace('.', '_')}_{clean_delta}_{chunk_index:06d}.parquet"
        else:
            return f"{effective_object_path}/{table_name.replace('.', '_')}_{chunk_index:06d}.parquet"
    
    def _update_chunk_audit(self, audit_log: Dict[str, Any], chunk_index: int,
                           total_records: int, extraction_start: datetime, 
                           effective_object_path: str):
        """Update audit log for processed chunk."""
        now_ist = datetime.now(IST)
        current_run_time = (now_ist - extraction_start).total_seconds()
        
        audit_log.update({
            "total_records": total_records,
            "status": "RUNNING",
            "task_endts": now_ist.strftime(DATETIMEFORMAT),
            "task_exec_secs": current_run_time,
            "extraction_time": current_run_time,
            "restart_point": chunk_index + 1,
            "minio_filepath": effective_object_path,
        })
        
        self.audit_manager.update_audit_record(audit_log)
    
    def _finalize_audit(self, audit_log: Dict[str, Any], extraction_start: datetime):
        """Finalize audit log."""
        final_ist = datetime.now(IST)
        final_time = (final_ist - extraction_start).total_seconds()
        
        audit_log.update({
            "status": "COMPLETED",
            "task_endts": final_ist.strftime(DATETIMEFORMAT),
            "task_exec_secs": final_time,
            "extraction_time": final_time,
        })
        
        self.audit_manager.update_audit_record(audit_log)
```

## Airflow DAG Implementation

**orchestration/airflow_dags/oracle_to_minio_dag.py**
```python
"""Airflow DAG for Oracle to MinIO data extraction."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from orchestration.tasks.extraction_tasks import run_table_extraction, run_config_driven_extraction

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Single table extraction DAG
dag_single_table = DAG(
    'oracle_to_minio_single_table',
    default_args=default_args,
    description='Extract single Oracle table to MinIO',
    schedule_interval='@daily',
    catchup=False,
    tags=['oracle', 'minio', 'extraction'],
)

extract_table_task = PythonOperator(
    task_id='extract_table',
    python_callable=run_table_extraction,
    op_kwargs={
        'table_name': '{{ var.value.table_name }}',
        'business_loaddt': '{{ ds }}',
        'load_type': '{{ var.value.load_type }}',
    },
    dag=dag_single_table,
)

# Config-driven extraction DAG
dag_config_driven = DAG(
    'oracle_to_minio_config_driven',
    default_args=default_args,
    description='Extract multiple Oracle tables based on configuration',
    schedule_interval='@daily',
    catchup=False,
    tags=['oracle', 'minio', 'extraction', 'config-driven'],
)

extract_config_task = PythonOperator(
    task_id='extract_config_driven',
    python_callable=run_config_driven_extraction,
    op_kwargs={
        'config_path': '{{ var.value.config_path }}',
        'business_loaddt': '{{ ds }}',
    },
    dag=dag_config_driven,
)
```

## Task Functions

**orchestration/tasks/extraction_tasks.py**
```python
"""Airflow task functions for data extraction."""
import yaml
from typing import Dict, Any

from src.processors.data_extractor import DataExtractor
from src.core.config import setup_logger
from cdp_diapi_adapter import get_system_config

logger = setup_logger(__name__)

def run_table_extraction(table_name: str, business_loaddt: str, 
                        load_type: str = 'delta', **context) -> None:
    """Run extraction for a single table."""
    
    # Get configuration
    conn_config = get_system_config()
    oracle_config = conn_config.get("target", {})
    minio_config = conn_config.get("minio", {})
    audit_config = {
        "target": oracle_config,
        "schema": "uds",
        "audit_table": "AIRFLOW_CDP_DIAPI_RUN_LOG",
    }
    
    # Initialize extractor
    extractor = DataExtractor(oracle_config, minio_config, audit_config)
    
    # Run extraction
    extractor.extract_table_to_parquet(
        table_name=table_name,
        base_object_path=f"data/{table_name.replace('.', '_')}",
        business_loaddt=business_loaddt,
        load_type=load_type,
    )
    
    logger.info(f"Extraction completed for {table_name}")

def run_config_driven_extraction(config_path: str, business_loaddt: str, **context) -> None:
    """Run extraction based on configuration file."""
    
    # Load configuration
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)["uds_to_minio"]
    
    conn_config = get_system_config()
    oracle_config = conn_config.get("target", {})
    minio_config = conn_config.get("minio", {})
    
    audit_config = {
        "target": oracle_config,
        "schema": config_data.get("schema", "uds"),
        "audit_table": config_data.get("audit_config", {}).get("audit_table", "AIRFLOW_CDP_DIAPI_RUN_LOG"),
    }
    
    extractor = DataExtractor(oracle_config, minio_config, audit_config)
    
    # Process each object in configuration
    for obj_config in config_data.get("objects", []):
        if not obj_config.get("isactive", True):
            logger.info(f"Skipping inactive object: {obj_config.get('db_table')}")
            continue
        
        table_name = f"{obj_config.get('schema', config_data.get('schema'))}.{obj_config['db_table']}"
        output_path = obj_config.get("output_path", config_data.get("output_path"))
        load_type = obj_config.get("load_type", "delta")
        
        extractor.extract_table_to_parquet(
            table_name=table_name,
            base_object_path=output_path,
            business_loaddt=business_loaddt,
            chunk_size=obj_config.get("chunksize", config_data.get("chunksize", 100_000)),
            order_by=obj_config.get("ORDER_BY"),
            load_type=load_type,
            delta_column=obj_config.get("delta_column") if load_type == 'historic' else None,
            sub_folder=obj_config.get("sub_folder") if load_type == 'historic' else None,
        )
        
        logger.info(f"Extraction completed for {table_name}")
```

## Key Benefits of This Refactoring:

1. **Modularity**: Each component has a single responsibility (database, storage, audit, processing)[1]
2. **Reusability**: Components can be easily imported and used in different contexts[1]
3. **SOLID Principles**: 
   - Single Responsibility: Each class handles one concern
   - Open/Closed: Easy to extend without modifying existing code
   - Dependency Inversion: High-level modules don't depend on low-level modules
4. **Airflow Integration**: Clean task functions that can be easily orchestrated[1]
5. **Configuration-Driven**: Supports both single table and config-driven extraction[1]
6. **Error Handling**: Proper exception handling and retry mechanisms[1]
7. **Testability**: Each component can be independently tested[1]

This structure allows you to easily create Airflow DAGs for different extraction scenarios while maintaining clean, reusable code that follows best practices for data engineering pipelines.

[1](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/90742002/220df2cd-a8b5-41aa-b388-0082fe249167/paste.txt)
