Here are the two files as requested: a complete YAML config and a single Python script wired to that config, using your `RETAIL_GDM_CUST_DIM` columns and MinIO layout.

***

## File 1: `config.yaml`

```yaml
trino:
  host: trino-coordinator          # or localhost if running locally
  port: 8080
  user: ingestion_user
  catalog_staging: hive            # catalog for temp table on Parquet
  catalog_iceberg: minio           # Iceberg catalog (your "minio" catalog)
  schema_log: meta                 # schema where ingestion_log will live

minio:
  bucket: sbi-uds-cdp-raw
  base_prefix: segmentation/segmentdb_6550/input_data/customer_data/retail_gdm_cust_dim/history
  folder_date_format: "%Y%m%d"     # folders like .../20230512/

table:
  # Fully-qualified Iceberg target (existing or to be created separately)
  iceberg_table: "minio.default.retail_gdm_cust_dim"

  # Hive schema where temp table is created
  staging_schema: "default"

  # Upsert behavior
  key_columns:
    - "CIF"
  date_column: "AUD_DT"

  # Which modes this run should use
  full_load: false
  delta_load: true

  # Columns present in Parquet & staging table (Hive temp table)
  input_columns:
    - { name: "CUST_NBR",            type: "VARCHAR(16)" }
    - { name: "CUST_NBR_CHKDGT",     type: "VARCHAR(1)" }
    - { name: "CIF",                 type: "VARCHAR(17)" }
    - { name: "CUST_TYP_CD",         type: "VARCHAR(20)" }
    - { name: "HOME_BRCH_NBR",       type: "VARCHAR(20)" }
    - { name: "TIER_CUST_TYP",       type: "VARCHAR(20)" }
    - { name: "CUST_STS",            type: "VARCHAR(20)" }
    - { name: "LNG_CD",              type: "VARCHAR(20)" }
    - { name: "VIP_CD",              type: "VARCHAR(20)" }
    - { name: "BRTH_DT",             type: "DATE" }
    - { name: "CUST_RSK_GRD",        type: "VARCHAR(20)" }
    - { name: "GNDR_CD",             type: "VARCHAR(5)" }
    - { name: "OFC_ADDR_LN_1",       type: "VARCHAR(255)" }
    - { name: "OFC_ADDR_LN_2",       type: "VARCHAR(255)" }
    - { name: "OCCUPNCY_CD",         type: "VARCHAR(20)" }
    - { name: "OFC_PST_CD",          type: "VARCHAR(20)" }
    - { name: "FRST_NM",             type: "VARCHAR(50)" }
    - { name: "MID_NM",              type: "VARCHAR(50)" }
    - { name: "LST_NM",              type: "VARCHAR(50)" }
    - { name: "FULL_NM",             type: "VARCHAR(255)" }
    - { name: "MOBILE_NBR",          type: "VARCHAR(30)" }
    - { name: "STATE_CD",            type: "VARCHAR(20)" }
    - { name: "EMAIL_ADDR_1",        type: "VARCHAR(100)" }
    - { name: "EMAIL_ADDR_2",        type: "VARCHAR(100)" }
    - { name: "DEATH_DT",            type: "DATE" }
    - { name: "EMP_FROM_DT",         type: "DATE" }
    - { name: "SHRT_NM",             type: "VARCHAR(50)" }
    - { name: "EMPLYR_NM",           type: "VARCHAR(100)" }
    - { name: "AUD_LOAD_TS",         type: "TIMESTAMP" }
    - { name: "AUD_DT",              type: "DATE" }
    - { name: "EXPRY_DT",            type: "DATE" }
    - { name: "UDS_LOAD_TS",         type: "TIMESTAMP" }
    - { name: "VALID_MOBILE_NO",     type: "VARCHAR(20)" }
    - { name: "MOBILE_NO_VALID_FLG", type: "VARCHAR(5)" }
    - { name: "EMAIL",               type: "VARCHAR(50)" }
    - { name: "EMAIL_VALID_FLG",     type: "VARCHAR(5)" }

  # Columns written into the Iceberg table (can be same as input or subset)
  output_columns:
    - "CUST_NBR"
    - "CUST_NBR_CHKDGT"
    - "CIF"
    - "CUST_TYP_CD"
    - "HOME_BRCH_NBR"
    - "TIER_CUST_TYP"
    - "CUST_STS"
    - "LNG_CD"
    - "VIP_CD"
    - "BRTH_DT"
    - "CUST_RSK_GRD"
    - "GNDR_CD"
    - "OFC_ADDR_LN_1"
    - "OFC_ADDR_LN_2"
    - "OCCUPNCY_CD"
    - "OFC_PST_CD"
    - "FRST_NM"
    - "MID_NM"
    - "LST_NM"
    - "FULL_NM"
    - "MOBILE_NBR"
    - "STATE_CD"
    - "EMAIL_ADDR_1"
    - "EMAIL_ADDR_2"
    - "DEATH_DT"
    - "EMP_FROM_DT"
    - "SHRT_NM"
    - "EMPLYR_NM"
    - "AUD_LOAD_TS"
    - "AUD_DT"
    - "EXPRY_DT"
    - "UDS_LOAD_TS"
    - "VALID_MOBILE_NO"
    - "MOBILE_NO_VALID_FLG"
    - "EMAIL"
    - "EMAIL_VALID_FLG"
```

***

## File 2: `ingest_minio_to_iceberg.py`

```python
import argparse
import datetime
import logging
import sys
from dataclasses import dataclass
from typing import List, Optional, Dict

import yaml
import trino


# ---------- Logging ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("minio_iceberg_ingester")


# ---------- Data classes ----------

@dataclass
class TrinoConfig:
    host: str
    port: int
    user: str
    catalog_iceberg: str
    catalog_staging: str
    schema_log: str


@dataclass
class MinioConfig:
    bucket: str
    base_prefix: str          # e.g. "segmentation/.../history"
    folder_date_format: str   # "%Y%m%d"


@dataclass
class TableConfig:
    staging_schema: str           # Hive schema for temp tables
    iceberg_table: str            # fully-qualified, e.g. "minio.default.retail_gdm_cust_dim"

    key_columns: List[str]
    date_column: str
    full_load: bool
    delta_load: bool

    input_columns: List[Dict]     # [{name, type}, ...]
    output_columns: List[str]     # ["CIF", "AUD_DT", ...]


# ---------- Helpers ----------

def load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def create_trino_conn(trino_cfg: TrinoConfig):
    return trino.dbapi.connect(
        host=trino_cfg.host,
        port=trino_cfg.port,
        user=trino_cfg.user,
        http_scheme="http",
    )


def fetchall(conn, sql: str, params: Optional[List] = None) -> List[tuple]:
    logger.debug("Executing SQL (fetchall): %s", sql)
    cur = conn.cursor()
    try:
        cur.execute(sql, params or [])
        return cur.fetchall()
    finally:
        cur.close()


def execute(conn, sql: str, params: Optional[List] = None):
    logger.debug("Executing SQL (execute): %s", sql)
    cur = conn.cursor()
    try:
        cur.execute(sql, params or [])
        _ = cur.fetchall()  # Trino DB-API expects result consumption
    finally:
        cur.close()


# ---------- Log table management ----------

def ensure_log_table(conn, trino_cfg: TrinoConfig):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {trino_cfg.catalog_iceberg}.{trino_cfg.schema_log}.ingestion_log (
        target_table   VARCHAR,
        folder_date    VARCHAR,
        mode           VARCHAR,
        status         VARCHAR,
        run_id         VARCHAR,
        processed_at   TIMESTAMP,
        row_count      BIGINT,
        error_message  VARCHAR
    )
    """
    execute(conn, sql)


def log_start(conn, trino_cfg: TrinoConfig,
              table_cfg: TableConfig, folder_date: str, mode: str, run_id: str):
    sql = f"""
    INSERT INTO {trino_cfg.catalog_iceberg}.{trino_cfg.schema_log}.ingestion_log (
        target_table,
        folder_date,
        mode,
        status,
        run_id,
        processed_at,
        row_count,
        error_message
    )
    VALUES (?, ?, ?, 'STARTED', ?, current_timestamp, NULL, NULL)
    """
    params = [
        table_cfg.iceberg_table,
        folder_date,
        mode,
        run_id,
    ]
    execute(conn, sql, params)


def log_success(conn, trino_cfg: TrinoConfig,
                table_cfg: TableConfig, folder_date: str, mode: str,
                run_id: str, row_count: int):
    sql = f"""
    UPDATE {trino_cfg.catalog_iceberg}.{trino_cfg.schema_log}.ingestion_log
    SET status = 'SUCCESS',
        processed_at = current_timestamp,
        row_count = ?,
        error_message = NULL
    WHERE target_table = ?
      AND folder_date  = ?
      AND mode         = ?
      AND run_id       = ?
      AND status       = 'STARTED'
    """
    params = [
        row_count,
        table_cfg.iceberg_table,
        folder_date,
        mode,
        run_id,
    ]
    execute(conn, sql, params)


def log_failure(conn, trino_cfg: TrinoConfig,
                table_cfg: TableConfig, folder_date: str, mode: str,
                run_id: str, error_message: str):
    sql = f"""
    UPDATE {trino_cfg.catalog_iceberg}.{trino_cfg.schema_log}.ingestion_log
    SET status = 'FAILED',
        processed_at = current_timestamp,
        error_message = ?
    WHERE target_table = ?
      AND folder_date  = ?
      AND mode         = ?
      AND run_id       = ?
      AND status       = 'STARTED'
    """
    params = [
        error_message[:1000],
        table_cfg.iceberg_table,
        folder_date,
        mode,
        run_id,
    ]
    execute(conn, sql, params)


def get_successful_folders(conn, trino_cfg: TrinoConfig,
                           table_cfg: TableConfig, mode: str) -> List[str]:
    sql = f"""
    SELECT DISTINCT folder_date
    FROM {trino_cfg.catalog_iceberg}.{trino_cfg.schema_log}.ingestion_log
    WHERE target_table = ?
      AND mode         = ?
      AND status       = 'SUCCESS'
    """
    params = [table_cfg.iceberg_table, mode]
    rows = fetchall(conn, sql, params)
    return [r[0] for r in rows]


# ---------- Folder discovery (replace with your real listing if needed) ----------

def list_available_folders(conn, trino_cfg: TrinoConfig,
                           minio_cfg: MinioConfig) -> List[str]:
    """
    Discover YYYYMMDD folders under base_prefix using hive catalog.

    NOTE: This uses a pseudo system.files table. If your hive catalog
    does not expose such a table, replace this function with MinIO API
    listing or your own metadata table.
    """
    sql = f"""
    SELECT DISTINCT regexp_extract(path, '.*/(\\d{{8}})/.*', 1) AS folder
    FROM {trino_cfg.catalog_staging}.system.files
    WHERE path LIKE 's3a://{minio_cfg.bucket}/{minio_cfg.base_prefix}/%'
      AND regexp_extract(path, '.*/(\\d{{8}})/.*', 1) IS NOT NULL
    """
    rows = fetchall(conn, sql)
    folders = sorted({r[0] for r in rows})
    return folders


# ---------- Staging table management ----------

def build_staging_table_name(table_cfg: TableConfig, folder_date: str) -> str:
    return f"stg_{table_cfg.iceberg_table.split('.')[-1]}_{folder_date}"


def create_staging_table(conn, trino_cfg: TrinoConfig,
                         minio_cfg: MinioConfig, table_cfg: TableConfig,
                         folder_date: str):
    staging_table_name = build_staging_table_name(table_cfg, folder_date)

    # Build columns from YAML input_columns
    column_ddls = []
    for col in table_cfg.input_columns:
        col_name = col["name"]
        col_type = col["type"]
        column_ddls.append(f"{col_name} {col_type}")
    columns_ddl = ",\n        ".join(column_ddls)

    folder_path = f"s3a://{minio_cfg.bucket}/{minio_cfg.base_prefix}/{folder_date}/"

    sql_drop = f"""
    DROP TABLE IF EXISTS {trino_cfg.catalog_staging}.{table_cfg.staging_schema}.{staging_table_name}
    """
    execute(conn, sql_drop)

    sql_create = f"""
    CREATE TABLE {trino_cfg.catalog_staging}.{table_cfg.staging_schema}.{staging_table_name} (
        {columns_ddl}
    )
    WITH (
        external_location = '{folder_path}',
        format = 'PARQUET'
    )
    """
    execute(conn, sql_create)
    logger.info(
        "Created staging table %s.%s.%s on %s",
        trino_cfg.catalog_staging,
        table_cfg.staging_schema,
        staging_table_name,
        folder_path,
    )


def drop_staging_table(conn, trino_cfg: TrinoConfig, table_cfg: TableConfig, folder_date: str):
    staging_table_name = build_staging_table_name(table_cfg, folder_date)
    sql_drop = f"""
    DROP TABLE IF EXISTS {trino_cfg.catalog_staging}.{table_cfg.staging_schema}.{staging_table_name}
    """
    execute(conn, sql_drop)


def validate_staging_date(conn, trino_cfg: TrinoConfig,
                          table_cfg: TableConfig, folder_date: str,
                          date_column: str, folder_date_format: str):
    staging_table_name = build_staging_table_name(table_cfg, folder_date)
    iso_date = datetime.datetime.strptime(folder_date, folder_date_format).date().isoformat()

    sql = f"""
    SELECT DISTINCT {date_column}
    FROM {trino_cfg.catalog_staging}.{table_cfg.staging_schema}.{staging_table_name}
    """
    rows = fetchall(conn, sql)
    distinct_dates = {str(r[0]) for r in rows}

    if len(distinct_dates) == 0:
        raise RuntimeError(f"No rows found in staging for folder {folder_date}")

    if distinct_dates != {iso_date}:
        raise RuntimeError(
            f"Staging {date_column} mismatch for folder {folder_date}: "
            f"found {distinct_dates}, expected only {iso_date}"
        )

    logger.info(
        "Staging %s validated for folder %s (%s)",
        date_column,
        folder_date,
        iso_date,
    )


# ---------- MERGE into Iceberg ----------

def run_merge(conn, trino_cfg: TrinoConfig,
              table_cfg: TableConfig, folder_date: str,
              folder_date_format: str) -> int:
    staging_table_name = build_staging_table_name(table_cfg, folder_date)
    iso_date = datetime.datetime.strptime(folder_date, folder_date_format).date().isoformat()

    # ON clause: keys + date
    key_eqs = [f"t.{k} = s.{k}" for k in table_cfg.key_columns]
    on_clause = " AND ".join(key_eqs)
    date_col = table_cfg.date_column
    on_clause += f" AND t.{date_col} = s.{date_col}"

    # non-key output columns to update
    key_set = set(table_cfg.key_columns + [date_col])
    non_key_cols = [c for c in table_cfg.output_columns if c not in key_set]
    set_clauses = [f"{col} = s.{col}" for col in non_key_cols]
    set_sql = ", ".join(set_clauses) if set_clauses else ""

    # insert columns/values from output_columns
    insert_cols_sql = ", ".join(table_cfg.output_columns)
    insert_vals_sql = ", ".join([f"s.{c}" for c in table_cfg.output_columns])

    merge_sql = f"""
    MERGE INTO {table_cfg.iceberg_table} t
    USING (
        SELECT *
        FROM {trino_cfg.catalog_staging}.{table_cfg.staging_schema}.{staging_table_name}
        WHERE {date_col} = DATE '{iso_date}'
    ) s
    ON {on_clause}
    {"WHEN MATCHED THEN UPDATE SET " + set_sql if set_sql else ""}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols_sql})
        VALUES ({insert_vals_sql})
    """
    execute(conn, merge_sql)

    count_sql = f"""
    SELECT COUNT(*)
    FROM {trino_cfg.catalog_staging}.{table_cfg.staging_schema}.{staging_table_name}
    WHERE {date_col} = DATE '{iso_date}'
    """
    rows = fetchall(conn, count_sql)
    row_count = rows[0][0] if rows else 0
    return row_count


# ---------- Orchestration (full vs delta) ----------

def pick_folders_to_process(
    all_folders: List[str],
    success_folders_full: List[str],
    success_folders_delta: List[str],
    table_cfg: TableConfig,
) -> Dict[str, List[str]]:
    result = {"full": [], "delta": []}

    if table_cfg.full_load:
        remaining = sorted(set(all_folders) - set(success_folders_full))
        result["full"] = remaining

    if table_cfg.delta_load and all_folders:
        latest = max(all_folders)
        if latest not in success_folders_delta:
            result["delta"] = [latest]

    return result


def run_for_table(conn, trino_cfg: TrinoConfig,
                  minio_cfg: MinioConfig, table_cfg: TableConfig):
    ensure_log_table(conn, trino_cfg)

    all_folders = list_available_folders(conn, trino_cfg, minio_cfg)
    if not all_folders:
        logger.info("No folders found under %s/%s", minio_cfg.bucket, minio_cfg.base_prefix)
        return

    success_full = get_successful_folders(conn, trino_cfg, table_cfg, mode="FULL")
    success_delta = get_successful_folders(conn, trino_cfg, table_cfg, mode="DELTA")

    todo = pick_folders_to_process(all_folders, success_full, success_delta, table_cfg)
    logger.info("Folders to process (full): %s", todo["full"])
    logger.info("Folders to process (delta): %s", todo["delta"])

    # FULL mode
    for folder in todo["full"]:
        run_id = f"FULL-{folder}"
        mode = "FULL"
        logger.info("Starting FULL load for folder %s", folder)
        try:
            log_start(conn, trino_cfg, table_cfg, folder, mode, run_id)
            create_staging_table(conn, trino_cfg, minio_cfg, table_cfg, folder)
            validate_staging_date(conn, trino_cfg, table_cfg, folder,
                                  table_cfg.date_column, minio_cfg.folder_date_format)
            row_count = run_merge(conn, trino_cfg, table_cfg, folder,
                                  minio_cfg.folder_date_format)
            log_success(conn, trino_cfg, table_cfg, folder, mode, run_id, row_count)
            logger.info("FULL load success for folder %s, rows=%s", folder, row_count)
        except Exception as e:
            logger.exception("FULL load failed for folder %s", folder)
            log_failure(conn, trino_cfg, table_cfg, folder, mode, run_id, str(e))
        finally:
            drop_staging_table(conn, trino_cfg, table_cfg, folder)

    # DELTA mode
    for folder in todo["delta"]:
        run_id = f"DELTA-{folder}"
        mode = "DELTA"
        logger.info("Starting DELTA load for folder %s", folder)
        try:
            log_start(conn, trino_cfg, table_cfg, folder, mode, run_id)
            create_staging_table(conn, trino_cfg, minio_cfg, table_cfg, folder)
            validate_staging_date(conn, trino_cfg, table_cfg, folder,
                                  table_cfg.date_column, minio_cfg.folder_date_format)
            row_count = run_merge(conn, trino_cfg, table_cfg, folder,
                                  minio_cfg.folder_date_format)
            log_success(conn, trino_cfg, table_cfg, folder, mode, run_id, row_count)
            logger.info("DELTA load success for folder %s, rows=%s", folder, row_count)
        except Exception as e:
            logger.exception("DELTA load failed for folder %s", folder)
            log_failure(conn, trino_cfg, table_cfg, folder, mode, run_id, str(e))
        finally:
            drop_staging_table(conn, trino_cfg, table_cfg, folder)


# ---------- CLI ----------

def parse_args():
    p = argparse.ArgumentParser(description="Ingest MinIO Parquet folders into Iceberg via Trino.")
    p.add_argument("--config", required=True, help="Path to YAML config")
    return p.parse_args()


def main():
    args = parse_args()
    cfg = load_yaml(args.config)

    trino_cfg = TrinoConfig(
        host=cfg["trino"]["host"],
        port=int(cfg["trino"]["port"]),
        user=cfg["trino"]["user"],
        catalog_iceberg=cfg["trino"]["catalog_iceberg"],
        catalog_staging=cfg["trino"]["catalog_staging"],
        schema_log=cfg["trino"]["schema_log"],
    )

    minio_cfg = MinioConfig(
        bucket=cfg["minio"]["bucket"],
        base_prefix=cfg["minio"]["base_prefix"].strip("/"),
        folder_date_format=cfg["minio"]["folder_date_format"],
    )

    table_cfg = TableConfig(
        staging_schema=cfg["table"]["staging_schema"],
        iceberg_table=cfg["table"]["iceberg_table"],
        key_columns=cfg["table"]["key_columns"],
        date_column=cfg["table"]["date_column"],
        full_load=bool(cfg["table"]["full_load"]),
        delta_load=bool(cfg["table"]["delta_load"]),
        input_columns=cfg["table"]["input_columns"],
        output_columns=cfg["table"]["output_columns"],
    )

    conn = None
    try:
        conn = create_trino_conn(trino_cfg)
        run_for_table(conn, trino_cfg, minio_cfg, table_cfg)
    except Exception:
        logger.exception("Fatal error in ingestion")
        sys.exit(1)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
```

You can now:

- Adjust schemas/columns purely in `config.yaml`.  
- Run with:  

```bash
python ingest_minio_to_iceberg.py --config config.yaml
```
