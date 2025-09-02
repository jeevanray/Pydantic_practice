import io
import os
import time
import hashlib
import logging
import threading
from pathlib import Path
from typing import Any, Dict, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import urllib3
import pandas as pd
from minio import Minio
from tenacity import retry, stop_after_attempt, wait_exponential
from minio.error import S3Error


class MinioConnectionError(Exception):
    pass


class MinioOperationError(Exception):
    pass


class MinioHandler(Minio):
    """
    Extended MinIO client with:
    - Built-in health checks
    - File/DataFrame upload (with MD5 metadata)
    - Large DataFrame parallel chunked upload
    - Large file multipart upload
    - Retry logic for all uploads
    """

    def __init__(self, config: Dict[str, Any]):
        required_keys = ['endpoint', 'access_key', 'secret_key']
        missing_keys = [k for k in required_keys if k not in config]
        if missing_keys:
            raise ValueError(f"Missing required config keys: {missing_keys}")

        endpoint = config["endpoint"]
        access_key = config["access_key"]
        secret_key = config["secret_key"]
        secure = config.get("secure", True)
        region = config.get("region")
        self.bucket_name = config.get("bucket_name", "sbi-test")

        # Disable SSL verification warnings
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        http_client = urllib3.PoolManager(cert_reqs="CERT_NONE")

        super().__init__(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
            http_client=http_client,
        )

        self.logger = self._setup_logging(endpoint)
        self._lock = threading.RLock()
        self._bucket_exists_cache = {}

        try:
            self._validate_connection()
            self._ensure_bucket_exists(self.bucket_name)
        except Exception as e:
            self.logger.error(f"Failed to initialize MinIOHandler: {e}")
            raise MinioConnectionError(str(e))

    # ---------- Helpers ----------

    def _setup_logging(self, endpoint: str) -> logging.Logger:
        logger = logging.getLogger(f"MinioHandler-{endpoint}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _validate_connection(self) -> None:
        try:
            buckets = list(self.list_buckets())
            self.logger.debug(f"Validated connection. Found {len(buckets)} buckets")
        except Exception as e:
            raise MinioConnectionError(f"Cannot connect to MinIO server: {e}")

    def _ensure_bucket_exists(self, bucket_name: str) -> None:
        if bucket_name in self._bucket_exists_cache:
            return
        if not self.bucket_exists(bucket_name):
            self.make_bucket(bucket_name)
            self.logger.info(f"Created bucket '{bucket_name}'")
        self._bucket_exists_cache[bucket_name] = True

    def _calculate_md5(self, data: Union[bytes, str, io.BytesIO]) -> str:
        md5 = hashlib.md5()
        if isinstance(data, bytes):
            md5.update(data)
        elif isinstance(data, str) and os.path.isfile(data):
            with open(data, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    md5.update(chunk)
        elif isinstance(data, io.BytesIO):
            while True:
                chunk = data.read(8192)
                if not chunk:
                    break
                md5.update(chunk)
            data.seek(0)
        else:
            raise ValueError("Unsupported type for MD5 calculation")
        return md5.hexdigest()

    # ---------- Monitoring ----------

    def health_check(self) -> Dict[str, Any]:
        try:
            start = time.time()
            buckets = list(self.list_buckets())
            bucket_accessible = self.bucket_exists(self.bucket_name)
            latency = (time.time() - start) * 1000
            return {
                "status": "healthy",
                "endpoint": self._endpoint,
                "bucket": self.bucket_name,
                "bucket_accessible": bucket_accessible,
                "buckets": len(buckets),
                "latency_ms": round(latency, 2),
                "ts": time.time(),
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e), "ts": time.time()}

    # ---------- Retry wrapper ----------

    def _retryable(method):
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            reraise=True,
        )(method)

    # ---------- Uploads ----------

    @_retryable
    def upload_file(
        self,
        local_path: str,
        object_path: Optional[str] = None,
        bucket_name: Optional[str] = None,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        bucket = bucket_name or self.bucket_name
        self._ensure_bucket_exists(bucket)

        if not object_path:
            object_path = Path(local_path).name

        md5_hash = self._calculate_md5(local_path)
        metadata = metadata or {}
        metadata["Content-MD5"] = md5_hash

        with self._lock:
            self.fput_object(bucket, object_path, local_path, content_type, metadata)

        self.logger.info(f"Uploaded '{local_path}' → {bucket}/{object_path}")
        return object_path

    @_retryable
    def upload_dataframe(
        self,
        df: pd.DataFrame,
        object_path: str,
        fmt: str = "csv",
        bucket_name: Optional[str] = None,
        **kwargs,
    ) -> str:
        """
        Uploads small/medium DataFrames in memory.
        """
        bucket = bucket_name or self.bucket_name
        self._ensure_bucket_exists(bucket)

        buffer = io.BytesIO()
        if fmt == "csv":
            df.to_csv(buffer, index=False, **kwargs)
            content_type = "text/csv"
        elif fmt == "parquet":
            df.to_parquet(buffer, **kwargs)
            content_type = "application/octet-stream"
        elif fmt == "json":
            df.to_json(buffer, **kwargs)
            content_type = "application/json"
        elif fmt == "excel":
            df.to_excel(buffer, index=False, **kwargs)
            content_type = (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            raise ValueError(f"Unsupported format: {fmt}")

        buffer.seek(0)
        data = buffer.getvalue()
        md5_hash = self._calculate_md5(data)

        metadata = {"Content-MD5": md5_hash, "X-Format": fmt}
        with self._lock:
            self.put_object(bucket, object_path, io.BytesIO(data), len(data), content_type, metadata)

        self.logger.info(f"Uploaded DataFrame → {bucket}/{object_path} ({fmt})")
        return object_path

    @_retryable
    def upload_large_dataframe(
        self,
        df: pd.DataFrame,
        object_path: str,
        chunk_size: int = 50000,
        fmt: str = "csv",
        bucket_name: Optional[str] = None,
        max_workers: int = 4,
        **kwargs,
    ) -> str:
        """
        Splits large DataFrame into chunks and uploads in parallel threads.
        """
        bucket = bucket_name or self.bucket_name
        self._ensure_bucket_exists(bucket)

        def upload_chunk(chunk_df, idx):
            buf = io.BytesIO()
            if fmt == "csv":
                chunk_df.to_csv(buf, index=False, header=(idx == 0), **kwargs)
                content_type = "text/csv"
            elif fmt == "json":
                chunk_df.to_json(buf, orient="records", lines=True, **kwargs)
                content_type = "application/json"
            else:
                raise ValueError("Large upload supported only for CSV/JSON")

            buf.seek(0)
            data = buf.getvalue()
            part_name = f"{object_path}.part{idx}"
            md5_hash = self._calculate_md5(data)

            metadata = {"Content-MD5": md5_hash, "X-Format": fmt, "Part-Index": str(idx)}

            with self._lock:
                self.put_object(bucket, part_name, io.BytesIO(data), len(data), content_type, metadata)

            return part_name

        futures = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i, start in enumerate(range(0, len(df), chunk_size)):
                chunk = df.iloc[start:start + chunk_size]
                futures.append(executor.submit(upload_chunk, chunk, i))

            for f in as_completed(futures):
                part = f.result()
                self.logger.info(f"Uploaded part: {part}")

        self.logger.info(f"Uploaded large DataFrame in {len(futures)} parts → {bucket}/{object_path}.part*")
        return object_path

    @_retryable
    def upload_large_file(
        self,
        local_path: str,
        object_path: Optional[str] = None,
        bucket_name: Optional[str] = None,
        part_size: int = 10 * 1024 * 1024,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Uses multipart upload for large files.
        """
        bucket = bucket_name or self.bucket_name
        self._ensure_bucket_exists(bucket)

        if not object_path:
            object_path = Path(local_path).name

        md5_hash = self._calculate_md5(local_path)
        metadata = metadata or {}
        metadata["Content-MD5"] = md5_hash

        with self._lock:
            self.fput_object(
                bucket,
                object_path,
                local_path,
                content_type=content_type,
                metadata=metadata,
                part_size=part_size,
            )
        self.logger.info(f"Uploaded large file '{local_path}' → {bucket}/{object_path}")
        return object_path
