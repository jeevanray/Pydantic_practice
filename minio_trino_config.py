trino:
  host: trino-coordinator
  port: 8080
  user: ingestion_user
  catalog_iceberg: minio
  catalog_staging: hive
  schema_log: meta

minio:
  bucket: my-bucket
  base_prefix: raw/table_name   # folders like raw/table_name/20251225/

table:
  schema: prod
  name: customer_fact

  # ice/merge config
  key_columns: ["cif"]
  date_column: "aud_dt"    # DATE column in target table

  # yaml may support multiple date-like folders; here we use YYYYMMDD
  folder_date_format: "%Y%m%d"

  # behavior flags
  full_load: false
  delta_load: true
