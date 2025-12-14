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
    base_prefix: str          # e.g. "raw/table_name"
    folder_date_format: str   # "%Y%m%d"


@dataclass
class TableConfig:
    schema: str               # iceberg schema
    name: str                 # iceberg table name
    key_columns: List[str]
    date_column: str          # e.g. "aud_dt"
    full_load: bool
    delta_load: bool


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
        _ = cur.fetchall()  # Trino DB-API requires consuming results
    finally:
        cur.close()


# ---------- Log table management ----------

def ensure_log_table(conn, trino_cfg: TrinoConfig):
    """
    Create ingestion log table if not exists:
    minio.meta.ingestion_log
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {trino_cfg.catalog_iceberg}.{trino_cfg.schema_log}.ingestion_log (
        target_catalog VARCHAR,
        target_schema  VARCHAR,
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
        target_catalog, target_schema, target_table,
        folder_date, mode, status, run_id, processed_at, row_count, error_message
    )
    VALUES (?, ?, ?, ?, ?, 'STARTED', ?, current_timestamp, NULL, NULL)
    """
    params = [
        trino_cfg.catalog_iceberg,
        table_cfg.schema,
        table_cfg.name,
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
    WHERE target_catalog = ?
      AND target_schema  = ?
      AND target_table   = ?
      AND folder_date    = ?
      AND mode           = ?
      AND run_id         = ?
      AND status         = 'STARTED'
    """
    params = [
        row_count,
        trino_cfg.catalog_iceberg,
        table_cfg.schema,
        table_cfg.name,
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
    WHERE target_catalog = ?
      AND target_schema  = ?
      AND target_table   = ?
      AND folder_date    = ?
      AND mode           = ?
      AND run_id         = ?
      AND status = 'STARTED'
    """
    params = [
        error_message[:1000],
        trino_cfg.catalog_iceberg,
        table_cfg.schema,
        table_cfg.name,
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
    WHERE target_catalog = ?
      AND target_schema  = ?
      AND target_table   = ?
      AND mode           = ?
      AND status         = 'SUCCESS'
    """
    params = [
        trino_cfg.catalog_iceberg,
        table_cfg.schema,
        table_cfg.name,
        mode,
    ]
    rows = fetchall(conn, sql, params)
    return [r[0] for r in rows]


# ---------- Folder discovery via Trino/Hive ----------

def list_available_folders(conn, trino_cfg: TrinoConfig,
                           minio_cfg: MinioConfig) -> List[str]:
    """
    Discover YYYYMMDD folders under base_prefix using hive catalog.
    This assumes Hive catalog is configured over MinIO.

    We use `LIST` function (Trino path listing via system table) or
    a small hack: create an external table on base_prefix and query directory listing.
    For simplicity here, assume directories are directly under base_prefix and
    we use information_schema.files via hive connector if enabled.

    Replace this with your actual folder listing mechanism if needed.
    """
    # Simplest robust option: rely on an external tool (MinIO client) or a custom table.
    # Here we assume a metadata view exists:
    # hive.information_schema.files(bucket, prefix) -> returns path
    # This is pseudo; replace with your real listing logic.
    sql = f"""
    SELECT DISTINCT regexp_extract(path, '.*/(\\d{{8}})/.*', 1) AS folder
    FROM {trino_cfg.catalog_staging}.system.files
    WHERE path LIKE 's3a://{minio_cfg.bucket}/{minio_cfg.base_prefix}/%'
      AND regexp_extract(path, '.*/(\\d{{8}})/.*', 1) IS NOT NULL
    """
    rows = fetchall(conn, sql)
    folders = sorted({r[0] for r in rows})
    return folders


# ---------- Schema discovery from Iceberg ----------

def get_target_schema(conn, trino_cfg: TrinoConfig,
                      table_cfg: TableConfig) -> List[Dict]:
    """
    Return list of dicts: [{"column_name": ..., "data_type": ...}, ...]
    based on Iceberg table schema.
    """
    sql = f"""
    SELECT column_name, data_type
    FROM {trino_cfg.catalog_iceberg}.information_schema.columns
    WHERE table_schema = ?
      AND table_name   = ?
    ORDER BY ordinal_position
    """
    params = [table_cfg.schema, table_cfg.name]
    rows = fetchall(conn, sql, params)
    return [{"column_name": r[0], "data_type": r[1]} for r in rows]


# ---------- Staging table management ----------

def build_staging_table_name(table_cfg: TableConfig, folder_date: str) -> str:
    return f"stg_{table_cfg.name}_{folder_date}"


def create_staging_table(conn, trino_cfg: TrinoConfig,
                         minio_cfg: MinioConfig, table_cfg: TableConfig,
                         folder_date: str, target_schema_cols: List[Dict]):
    """
    Create/replace staging external table on single folder using hive catalog.
    """
    staging_table_name = build_staging_table_name(table_cfg, folder_date)

    # Build column DDL from target schema (can filter if you want)
    column_ddls = []
    for col in target_schema_cols:
        col_name = col["column_name"]
        col_type = col["data_type"]
        column_ddls.append(f"{col_name} {col_type}")
    columns_ddl = ",\n        ".join(column_ddls)

    folder_path = f"s3a://{minio_cfg.bucket}/{minio_cfg.base_prefix}/{folder_date}/"

    sql_drop = f"DROP TABLE IF EXISTS {trino_cfg.catalog_staging}.{table_cfg.schema}.{staging_table_name}"
    execute(conn, sql_drop)

    sql_create = f"""
    CREATE TABLE {trino_cfg.catalog_staging}.{table_cfg.schema}.{staging_table_name} (
        {columns_ddl}
    )
    WITH (
        external_location = '{folder_path}',
        format = 'PARQUET'
    )
    """
    execute(conn, sql_create)
    logger.info("Created staging table %s.%s.%s on %s",
                trino_cfg.catalog_staging, table_cfg.schema, staging_table_name, folder_path)


def drop_staging_table(conn, trino_cfg: TrinoConfig, table_cfg: TableConfig, folder_date: str):
    staging_table_name = build_staging_table_name(table_cfg, folder_date)
    sql_drop = f"DROP TABLE IF EXISTS {trino_cfg.catalog_staging}.{table_cfg.schema}.{staging_table_name}"
    execute(conn, sql_drop)


def validate_staging_date(conn, trino_cfg: TrinoConfig,
                          table_cfg: TableConfig, folder_date: str,
                          date_column: str, folder_date_format: str):
    """
    Ensure distinct date_column in staging matches folder date.
    """
    staging_table_name = build_staging_table_name(table_cfg, folder_date)
    iso_date = datetime.datetime.strptime(folder_date, folder_date_format).date().isoformat()

    sql = f"""
    SELECT DISTINCT {date_column}
    FROM {trino_cfg.catalog_staging}.{table_cfg.schema}.{staging_table_name}
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

    logger.info("Staging %s validated for folder %s (%s)",
                date_column, folder_date, iso_date)


# ---------- MERGE into Iceberg ----------

def run_merge(conn, trino_cfg: TrinoConfig,
              table_cfg: TableConfig, folder_date: str,
              folder_date_format: str, target_schema_cols: List[Dict]) -> int:
    """
    Execute MERGE INTO minio.schema.table USING hive.schema.stg_table.
    Return number of rows merged (approx via COUNT from staging).
    """
    staging_table_name = build_staging_table_name(table_cfg, folder_date)

    # derive iso date string
    iso_date = datetime.datetime.strptime(folder_date, folder_date_format).date().isoformat()

    # Build join condition on key columns
    key_eqs = [f"t.{k} = s.{k}" for k in table_cfg.key_columns]
    on_clause = " AND ".join(key_eqs)

    # Optional: Include date predicate in ON or WHERE for pruning
    # Here: only operate on target rows where AUD_DT = that date (per your semantics)
    date_col = table_cfg.date_column
    on_clause += f" AND t.{date_col} = s.{date_col}"

    # Build update set list: update all non-key columns
    key_set = set(table_cfg.key_columns + [date_col])
    non_key_cols = [c["column_name"] for c in target_schema_cols if c["column_name"] not in key_set]
    set_clauses = [f"{col} = s.{col}" for col in non_key_cols]
    set_sql = ", ".join(set_clauses) if set_clauses else ""

    # Build insert columns/values
    all_cols = [c["column_name"] for c in target_schema_cols]
    insert_cols_sql = ", ".join(all_cols)
    insert_vals_sql = ", ".join([f"s.{c}" for c in all_cols])

    # MERGE query
    # Filter staging to the date (s.date_column = DATE '<iso>')
    merge_sql = f"""
    MERGE INTO {trino_cfg.catalog_iceberg}.{table_cfg.schema}.{table_cfg.name} t
    USING (
        SELECT *
        FROM {trino_cfg.catalog_staging}.{table_cfg.schema}.{staging_table_name}
        WHERE {date_col} = DATE '{iso_date}'
    ) s
    ON {on_clause}
    {"WHEN MATCHED THEN UPDATE SET " + set_sql if set_sql else ""}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols_sql})
        VALUES ({insert_vals_sql})
    """

    execute(conn, merge_sql)

    # Approx row_count: count rows in staging for that date
    count_sql = f"""
    SELECT COUNT(*)
    FROM {trino_cfg.catalog_staging}.{table_cfg.schema}.{staging_table_name}
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
    table_cfg: TableConfig
) -> Dict[str, List[str]]:
    """
    Returns dict with "full" and "delta" keys listing folders to process.
    """
    result = {"full": [], "delta": []}

    if table_cfg.full_load:
        # Full mode: all unprocessed in full mode
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

    # get folders from MinIO via hive
    all_folders = list_available_folders(conn, trino_cfg, minio_cfg)
    if not all_folders:
        logger.info("No folders found under %s/%s", minio_cfg.bucket, minio_cfg.base_prefix)
        return

    success_full = get_successful_folders(conn, trino_cfg, table_cfg, mode="FULL")
    success_delta = get_successful_folders(conn, trino_cfg, table_cfg, mode="DELTA")

    todo = pick_folders_to_process(all_folders, success_full, success_delta, table_cfg)
    logger.info("Folders to process (full): %s", todo["full"])
    logger.info("Folders to process (delta): %s", todo["delta"])

    target_schema_cols = get_target_schema(conn, trino_cfg, table_cfg)
    if not target_schema_cols:
        raise RuntimeError(f"No schema found for {trino_cfg.catalog_iceberg}.{table_cfg.schema}.{table_cfg.name}")

    # FULL mode
    for folder in todo["full"]:
        run_id = f"FULL-{folder}"
        mode = "FULL"
        logger.info("Starting FULL load for folder %s", folder)
        try:
            log_start(conn, trino_cfg, table_cfg, folder, mode, run_id)
            create_staging_table(conn, trino_cfg, minio_cfg, table_cfg, folder, target_schema_cols)
            validate_staging_date(conn, trino_cfg, table_cfg, folder,
                                  table_cfg.date_column, minio_cfg.folder_date_format)
            row_count = run_merge(conn, trino_cfg, table_cfg, folder,
                                  minio_cfg.folder_date_format, target_schema_cols)
            log_success(conn, trino_cfg, table_cfg, folder, mode, run_id, row_count)
            logger.info("FULL load success for folder %s, rows=%s", folder, row_count)
        except Exception as e:
            logger.exception("FULL load failed for folder %s", folder)
            log_failure(conn, trino_cfg, table_cfg, folder, mode, run_id, str(e))
        finally:
            # Drop staging regardless of success/failure (optional)
            drop_staging_table(conn, trino_cfg, table_cfg, folder)

    # DELTA mode
    for folder in todo["delta"]:
        run_id = f"DELTA-{folder}"
        mode = "DELTA"
        logger.info("Starting DELTA load for folder %s", folder)
        try:
            log_start(conn, trino_cfg, table_cfg, folder, mode, run_id)
            create_staging_table(conn, trino_cfg, minio_cfg, table_cfg, folder, target_schema_cols)
            validate_staging_date(conn, trino_cfg, table_cfg, folder,
                                  table_cfg.date_column, minio_cfg.folder_date_format)
            row_count = run_merge(conn, trino_cfg, table_cfg, folder,
                                  minio_cfg.folder_date_format, target_schema_cols)
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
        folder_date_format=cfg["table"]["folder_date_format"],
    )

    table_cfg = TableConfig(
        schema=cfg["table"]["schema"],
        name=cfg["table"]["name"],
        key_columns=cfg["table"]["key_columns"],
        date_column=cfg["table"]["date_column"],
        full_load=bool(cfg["table"]["full_load"]),
        delta_load=bool(cfg["table"]["delta_load"]),
    )

    try:
        conn = create_trino_conn(trino_cfg)
        run_for_table(conn, trino_cfg, minio_cfg, table_cfg)
    except Exception as e:
        logger.exception("Fatal error in ingestion")
        sys.exit(1)
    finally:
        try:
            conn.close()  # type: ignore
        except Exception:
            pass


if __name__ == "__main__":
    main()
