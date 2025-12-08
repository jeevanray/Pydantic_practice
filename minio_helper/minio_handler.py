import io
import urllib3
from typing import Any, Dict, Optional
 
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
 
from minio_helper.logconfig import get_logger
 
logger = get_logger("minio_handler")
 
 
def init_minio_client(config: Dict[str, Any]) -> Minio:
    """Initialize and return a Minio client using the provided config dict.
 
    Required keys: endpoint, access_key, secret_key
    Optional: secure (bool), region
    """
    required = ["endpoint", "access_key", "secret_key"]
    for k in required:
        if k not in config:
            raise ValueError(f"Missing MinIO config key: {k}")
 
    minio_kwargs = {
        "endpoint": config["endpoint"],
        "access_key": config["access_key"],
        "secret_key": config["secret_key"],
        "secure": config.get("secure", True),
    }
    if config.get('secure', True):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
        minio_kwargs['http_client'] = http_client
    if "region" in config:
        minio_kwargs["region"] = config["region"]
 
    client = Minio(**minio_kwargs)
 
    # Ensure bucket exists if specified
    bucket = config.get("bucket_name")
    if bucket and not client.bucket_exists(bucket):
        client.make_bucket(bucket)
 
    return client
 
 
def upload_table(
    client: Minio,
    table: pa.Table,
    bucket_name: str,
    object_path: str,
    compression: Optional[str] = "snappy",
    part_size: int = 10 * 1024 * 1024,
) -> str:
    """Upload a PyArrow Table to MinIO using multipart put for best throughput.
 
    Serializes the table to parquet in-memory and streams to MinIO using put_object.
    `part_size` controls multipart chunking.
    Returns the object_path on success.
    """
    buffer = io.BytesIO()
    comp = compression or "snappy"
    pq.write_table(table, buffer, compression=comp)
    buffer.seek(0)
    data = buffer.read()
 
    # Minio put_object supports streaming via a file-like object with length and part_size
    bio = io.BytesIO(data)
    content_length = len(data)
 
    content_type = "application/octet-stream"
 
    # Use put_object which will automatically use multipart for large objects
    client.put_object(bucket_name, object_path, bio, content_length, content_type=content_type, part_size=part_size)
    logger.info(f"Uploaded parquet to {bucket_name}/{object_path} ({content_length} bytes)")
    return object_path
 
 
def delete_file(client: Minio, bucket_name: str, object_path: str) -> bool:
    """Delete an object from MinIO. Returns True if deleted or False if not found."""
    try:
        client.remove_object(bucket_name, object_path)
        logger.info(f"Deleted object {bucket_name}/{object_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to delete {bucket_name}/{object_path}: {e}")
        return False
 
 
if __name__ == "__main__":
    # small smoke test when run standalone (no network checks here)
    print("minio_handler functional module loaded")
 
 
# Compatibility wrapper: lightweight MinioHandler shim for older code that expects a class
class MinioHandler:
    """Small wrapper around the Minio client to preserve legacy imports.
 
    This provides minimal methods used across the codebase: upload_table (delegates to function),
    put_object/remove_object passthroughs and a no-op close(). Prefer using init_minio_client/upload_table.
    """
    def __init__(self, config: Dict[str, Any]):
        self._client = init_minio_client(config)
        self.endpoint = config.get("endpoint")
 
    def upload_table(self, table: pa.Table, bucket_name: str, object_path: str = None, **kwargs):
        if not bucket_name:
            raise ValueError("bucket_name must be provided")
        return upload_table(self._client, table, bucket_name, object_path, **kwargs)
 
    def put_object(self, bucket_name: str, object_path: str, data, length: int, **kwargs):
        return self._client.put_object(bucket_name, object_path, data, length, **kwargs)
 
    def remove_object(self, bucket_name: str, object_path: str):
        return self._client.remove_object(bucket_name, object_path)
 
    def close(self):
        # Minio client has no explicit close method; keep for compatibility
        return None
