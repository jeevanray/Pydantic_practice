import io
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union
from urllib.parse import unquote
import urllib3

import pandas as pd
from minio import Minio
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject
from minio.error import S3Error


class MinioConnectionError(Exception):
    """Custom exception for MinIO connection issues."""
    pass


class MinioOperationError(Exception):
    """Custom exception for MinIO operation failures."""
    pass


class MinioHandler:
    """
    A comprehensive handler for MinIO object storage operations.

    This class provides a robust interface for interacting with MinIO servers,
    including file operations, data management, and administrative tasks.

    Attributes:
        client (Minio): The MinIO client instance.
        bucket_name (str): The default bucket name for operations.
        logger (logging.Logger): Logger instance for operation tracking.
        _bucket_exists_cache (Dict[str, bool]): Cache for bucket existence checks.
        _lock (threading.RLock): Thread lock for safe concurrent operations.
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the MinioHandler.

        Args:
            config (Dict[str, Any]): A dictionary containing MinIO connection details.
        """
        required_keys = ['endpoint', 'access_key', 'secret_key']
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")

        self._config = config.copy()  # Make a copy to avoid modifying the original
        self.endpoint = self._config['endpoint']
        self.bucket_name = self._config.get('bucket_name', "sbi-test")
        self._config.pop('bucket_name', None)  # Remove bucket_name from config

        self.client: Optional[Minio] = None
        self.logger = self._setup_logging()
        self._bucket_exists_cache: Dict[str, bool] = {}
        self._lock = threading.RLock()

        # Initialize the connection
        self.connect()

    def __enter__(self):
        """
        Establishes a MinIO client connection for the `with` statement.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Cleans up resources by closing the client connection.
        """
        self.close()

    def connect(self) -> None:
        """
        Establishes a MinIO client connection manually.
        """
        with self._lock:
            if self.client:
                self.logger.debug("Connection already exists.")
                return

            try:
                # Disable SSL verification warnings
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')

                # Create a copy of config without bucket_name
                minio_config = {
                    'endpoint': self._config['endpoint'],
                    'access_key': self._config['access_key'],
                    'secret_key': self._config['secret_key'],
                    'secure': self._config.get('secure', True)
                }
                if 'region' in self._config:
                    minio_config['region'] = self._config['region']

                self.client = Minio(**minio_config, http_client=http_client)

                # Validate connection by checking bucket
                try:
                    if not self.client.bucket_exists(self.bucket_name):
                        self.logger.warning(f"Bucket {self.bucket_name} does not exist.")
                        raise MinioOperationError(f"Bucket {self.bucket_name} does not exist")
                    self.logger.info("MinioClient connection established successfully.")
                except Exception as bucket_error:
                    self.client = None
                    raise MinioOperationError(f"Bucket validation failed: {bucket_error}")

            except Exception as e:
                self.client = None
                self.logger.error(f"Failed to connect to Minio: {e}")
                raise MinioConnectionError(f"Failed to connect to Minio: {e}")

    def close(self):
        """
        Closes the MinIO client connection manually.
        """
        if self.client:
            print("MinioClient connection closed.")
            self.client = None

    # def __init__(
    #     self,
    #     endpoint: str,
    #     access_key: str,
    #     secret_key: str,
    #     bucket_name: str,
    #     secure: bool = True,
    #     region: Optional[str] = None
    # ) -> None:
        """
        Initialize MinIO handler with connection parameters.

        Args:
            endpoint: MinIO server endpoint (e.g., 'localhost:9000').
            access_key: Access key for authentication.
            secret_key: Secret key for authentication.
            bucket_name: Default bucket name for operations.
            secure: Whether to use HTTPS (default: True).
            region: Optional region specification.

        Raises:
            MinioConnectionError: If connection to MinIO server fails.
            MinioOperationError: If bucket creation/validation fails.
        """
        '''
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        self.logger = self._setup_logging()
        self._bucket_exists_cache: Dict[str, bool] = {}
        self._lock = threading.RLock()
        http_client = None

        try:
            # Initialize MinIO client
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
            self.client = Minio(
                endpoint=endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
                region=region,
                http_client=http_client
            )

            # Test connection and ensure bucket exists
            self._validate_connection()
            self._ensure_bucket_exists(bucket_name)

            self.logger.info(f"MinioHandler initialized successfully for bucket '{bucket_name}'")

        except Exception as e:
            self.logger.error(f"Failed to initialize MinioHandler: {str(e)}")
            raise MinioConnectionError(f"Failed to connect to MinIO server: {str(e)}")'''

    def _setup_logging(self) -> logging.Logger:
        """Set up logging for the MinIO handler."""
        logger = logging.getLogger(f"MinioHandler-{self.endpoint}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _validate_connection(self) -> None:
        """Validate connection to MinIO server."""
        try:
            # Simple operation to test connection
            list(self.client.list_buckets())
        except Exception as e:
            raise MinioConnectionError(f"Cannot connect to MinIO server: {str(e)}")

    def _ensure_bucket_exists(self, bucket_name: str) -> None:
        """Ensure bucket exists, create if it doesn't."""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                self.logger.info(f"Created bucket '{bucket_name}'")
            self._bucket_exists_cache[bucket_name] = True
        except Exception as e:
            raise MinioOperationError(f"Failed to ensure bucket exists: {str(e)}")

    def health_check(self) -> Dict[str, Any]:
        """
        Check the health of MinIO connection and bucket accessibility.

        Returns:
            Dict containing health status information.
        """
        try:
            start_time = time.time()
            if not self.client:
                raise ValueError("MinioClient not connected. Call connect() or use a `with` block.")

            if not self.bucket_name:
                self.logger.error("Bucket name not specified in the constructor.")
                return False
            # Test basic operations
            buckets = list(self.client.list_buckets())
            bucket_accessible = self.client.bucket_exists(self.bucket_name)

            # Test object listing (should work even if bucket is empty)
            objects = list(self.client.list_objects(self.bucket_name, recursive=False))

            response_time = time.time() - start_time

            return {
                "status": "healthy",
                "endpoint": self.endpoint,
                "bucket_name": self.bucket_name,
                "bucket_accessible": bucket_accessible,
                "total_buckets": len(buckets),
                "response_time_ms": round(response_time * 1000, 2),
                "timestamp": time.time()
            }

        except Exception as e:
            self.logger.error(f"Health check failed: {str(e)}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": time.time()
            }

    def read_file(
        self,
        object_path: str,
        return_type: str = 'bytes',
        bucket_name: Optional[str] = None
    ) -> Union[bytes, str, Dict[str, Any], pd.DataFrame]:
        """
        Read file content from MinIO.

        Args:
            object_path: Path to the object in MinIO.
            return_type: Type of return value ('bytes', 'string', 'json', 'dataframe').
            bucket_name: Optional bucket name (uses default if not specified).

        Returns:
            File content in the specified format.

        Raises:
            MinioOperationError: If file reading fails.
        """
        bucket = bucket_name or self.bucket_name

        try:
            response = self.client.get_object(bucket, object_path)
            data = response.read()
            response.close()
            response.release_conn()

            if return_type == 'bytes':
                return data
            elif return_type == 'string':
                return data.decode('utf-8')
            elif return_type == 'json':
                return json.loads(data.decode('utf-8'))
            elif return_type == 'dataframe':
                # Determine file format from extension
                ext = Path(object_path).suffix.lower()
                if ext == '.csv':
                    return pd.read_csv(io.BytesIO(data))
                elif ext in ['.parquet', '.pq']:
                    return pd.read_parquet(io.BytesIO(data))
                elif ext in ['.xlsx', '.xls']:
                    return pd.read_excel(io.BytesIO(data))
                else:
                    raise MinioOperationError(f"Unsupported file format for DataFrame: {ext}")
            else:
                raise ValueError(f"Unsupported return_type: {return_type}")

        except S3Error as e:
            self.logger.error(f"Failed to read file '{object_path}': {str(e)}")
            raise MinioOperationError(f"Failed to read file: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error reading file '{object_path}': {str(e)}")
            raise MinioOperationError(f"Unexpected error: {str(e)}")

    def download_file(
        self,
        object_path: str,
        local_path: str,
        overwrite: bool = False,
        progress_callback: Optional[callable] = None,
        bucket_name: Optional[str] = None
    ) -> str:
        """
        Download file from MinIO to local path.

        Args:
            object_path: Path to the object in MinIO.
            local_path: Local path where file should be saved.
            overwrite: Whether to overwrite existing local files.
            progress_callback: Optional callback function for progress tracking.
            bucket_name: Optional bucket name (uses default if not specified).

        Returns:
            Path to the downloaded file.

        Raises:
            MinioOperationError: If download fails.
            FileExistsError: If local file exists and overwrite is False.
        """
        bucket = bucket_name or self.bucket_name
        local_path_obj = Path(local_path)

        # Check if local file exists
        if local_path_obj.exists() and not overwrite:
            raise FileExistsError(f"Local file '{local_path}' already exists")

        # Create parent directories if they don't exist
        local_path_obj.parent.mkdir(parents=True, exist_ok=True)

        try:
            if progress_callback:
                # Download with progress tracking
                response = self.client.get_object(bucket, object_path)

                # Get object info for progress calculation
                obj_info = self.client.stat_object(bucket, object_path)
                total_size = obj_info.size
                downloaded = 0

                with open(local_path, 'wb') as f:
                    while True:
                        chunk = response.read(8192)  # 8KB chunks
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        progress_callback(downloaded, total_size)

                response.close()
                response.release_conn()
            else:
                # Simple download
                self.client.fget_object(bucket, object_path, local_path)

            self.logger.info(f"Downloaded '{object_path}' to '{local_path}'")
            return str(local_path_obj.absolute())

        except Exception as e:
            self.logger.error(f"Failed to download '{object_path}': {str(e)}")
            raise MinioOperationError(f"Failed to download file: {str(e)}")

    def delete_file(self, object_path: str, bucket_name: Optional[str] = None) -> bool:
        """
        Delete a specific file from MinIO.

        Args:
            object_path: Path to the object in MinIO.
            bucket_name: Optional bucket name (uses default if not specified).

        Returns:
            True if deletion was successful.

        Raises:
            MinioOperationError: If deletion fails.
        """
        bucket = bucket_name or self.bucket_name

        try:
            self.client.remove_object(bucket, object_path)
            self.logger.info(f"Deleted file '{object_path}' from bucket '{bucket}'")
            return True

        except Exception as e:
            self.logger.error(f"Failed to delete file '{object_path}': {str(e)}")
            raise MinioOperationError(f"Failed to delete file: {str(e)}")

    def delete_folder(
        self,
        folder_path: str,
        bucket_name: Optional[str] = None,
        batch_size: int = 1000
    ) -> int:
        """
        Delete a folder and all its contents recursively.

        Args:
            folder_path: Path to the folder in MinIO.
            bucket_name: Optional bucket name (uses default if not specified).
            batch_size: Number of objects to delete in each batch.

        Returns:
            Number of objects deleted.

        Raises:
            MinioOperationError: If deletion fails.
        """
        bucket = bucket_name or self.bucket_name
        folder_path = folder_path.rstrip('/') + '/'
        deleted_count = 0

        try:
            # Get all objects in the folder
            objects = self.client.list_objects(bucket, prefix=folder_path, recursive=True)

            # Batch delete for efficiency
            batch = []
            for obj in objects:
                batch.append(DeleteObject(obj.object_name))

                if len(batch) >= batch_size:
                    errors = list(self.client.remove_objects(bucket, batch))
                    if errors:
                        for error in errors:
                            self.logger.error(f"Failed to delete {error.object_name}: {error}")
                    deleted_count += len(batch) - len(errors)
                    batch = []

            # Delete remaining objects
            if batch:
                errors = list(self.client.remove_objects(bucket, batch))
                if errors:
                    for error in errors:
                        self.logger.error(f"Failed to delete {error.object_name}: {error}")
                deleted_count += len(batch) - len(errors)

            self.logger.info(f"Deleted folder '{folder_path}' with {deleted_count} objects")
            return deleted_count

        except Exception as e:
            self.logger.error(f"Failed to delete folder '{folder_path}': {str(e)}")
            raise MinioOperationError(f"Failed to delete folder: {str(e)}")

    def upload_file(
        self,
        local_path: str,
        object_path: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        content_type: Optional[str] = None,
        bucket_name: Optional[str] = None
    ) -> str:
        """
        Upload a file to MinIO.

        Args:
            local_path: Path to the local file.
            object_path: Destination path in MinIO (uses filename if not specified).
            metadata: Optional metadata to attach to the object.
            content_type: Optional content type (auto-detected if not specified).
            bucket_name: Optional bucket name (uses default if not specified).

        Returns:
            The object path in MinIO.

        Raises:
            MinioOperationError: If upload fails.
            FileNotFoundError: If local file doesn't exist.
        """
        bucket = bucket_name or self.bucket_name
        local_file = Path(local_path)

        if not local_file.exists():
            raise FileNotFoundError(f"Local file '{local_path}' not found")

        if not object_path:
            object_path = local_file.name

        try:
            # Auto-detect content type if not provided
            if not content_type:
                content_type = self._get_content_type(local_file.suffix)

            self.client.fput_object(
                bucket,
                object_path,
                str(local_file),
                content_type=content_type,
                metadata=metadata
            )

            self.logger.info(f"Uploaded '{local_path}' to '{object_path}'")
            return object_path

        except Exception as e:
            self.logger.error(f"Failed to upload '{local_path}': {str(e)}")
            raise MinioOperationError(f"Failed to upload file: {str(e)}")

    def upload_large_file(
        self,
        local_path: str,
        object_path: Optional[str] = None,
        part_size: int = 10 * 1024 * 1024,  # 10MB default
        progress_callback: Optional[callable] = None,
        metadata: Optional[Dict[str, str]] = None,
        bucket_name: Optional[str] = None
    ) -> str:
        """
        Upload large files using multipart upload with progress tracking.

        Args:
            local_path: Path to the local file.
            object_path: Destination path in MinIO.
            part_size: Size of each part in bytes (default: 10MB).
            progress_callback: Optional callback for progress tracking.
            metadata: Optional metadata to attach to the object.
            bucket_name: Optional bucket name (uses default if not specified).

        Returns:
            The object path in MinIO.

        Raises:
            MinioOperationError: If upload fails.
            FileNotFoundError: If local file doesn't exist.
        """
        bucket = bucket_name or self.bucket_name
        local_file = Path(local_path)

        if not local_file.exists():
            raise FileNotFoundError(f"Local file '{local_path}' not found")

        if not object_path:
            object_path = local_file.name

        try:
            file_size = local_file.stat().st_size

            # Use progress callback wrapper if provided
            if progress_callback:
                class ProgressWrapper:
                    def __init__(self, file_path: str, callback: callable, total_size: int):
                        self.file = open(file_path, 'rb')
                        self.callback = callback
                        self.total_size = total_size
                        self.uploaded = 0

                    def read(self, size: int) -> bytes:
                        data = self.file.read(size)
                        self.uploaded += len(data)
                        self.callback(self.uploaded, self.total_size)
                        return data

                    def close(self):
                        self.file.close()

                file_obj = ProgressWrapper(str(local_file), progress_callback, file_size)
            else:
                file_obj = open(str(local_file), 'rb')

            try:
                content_type = self._get_content_type(local_file.suffix)

                result = self.client.put_object(
                    bucket,
                    object_path,
                    file_obj,
                    length=file_size,
                    content_type=content_type,
                    metadata=metadata,
                    part_size=part_size
                )

                self.logger.info(f"Uploaded large file '{local_path}' to '{object_path}' (ETag: {result.etag})")
                return object_path

            finally:
                file_obj.close()

        except Exception as e:
            self.logger.error(f"Failed to upload large file '{local_path}': {str(e)}")
            raise MinioOperationError(f"Failed to upload large file: {str(e)}")

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        object_path: str,
        format: str = 'csv',
        chunk_size: Optional[int] = None,
        compression: Optional[str] = None,
        **kwargs
    ) -> Union[str, List[str]]:
        """
        Upload a Pandas DataFrame to MinIO.

        Args:
            df: The DataFrame to upload.
            object_path: Destination path in MinIO.
            format: File format ('csv', 'parquet', 'json', 'excel').
            chunk_size: Optional chunk size for large DataFrames.
            compression: Optional compression ('gzip', 'bz2', 'xz').
            **kwargs: Additional arguments passed to pandas methods.

        Returns:
            Object path(s) in MinIO (list if chunked).

        Raises:
            MinioOperationError: If upload fails.
            ValueError: If unsupported format is specified.
        """
        try:
            if chunk_size and len(df) > chunk_size:
                return self._upload_dataframe_chunked(df, object_path, format, chunk_size, compression, **kwargs)
            else:
                return self._upload_dataframe_single(df, object_path, format, compression, **kwargs)

        except Exception as e:
            self.logger.error(f"Failed to upload DataFrame: {str(e)}")
            raise MinioOperationError(f"Failed to upload DataFrame: {str(e)}")

    def _upload_dataframe_single(
        self,
        df: pd.DataFrame,
        object_path: str,
        format: str,
        compression: Optional[str] = None,
        **kwargs
    ) -> str:
        """Upload DataFrame as a single file."""
        buffer = io.BytesIO()

        if format == 'csv':
            df.to_csv(buffer, index=False, compression=compression, **kwargs)
            content_type = 'text/csv'
        elif format == 'parquet':
            df.to_parquet(buffer, compression=compression, **kwargs)
            content_type = 'application/octet-stream'
        elif format == 'json':
            df.to_json(buffer, compression=compression, **kwargs)
            content_type = 'application/json'
        elif format == 'excel':
            df.to_excel(buffer, index=False, **kwargs)
            content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        else:
            raise ValueError(f"Unsupported format: {format}")

        buffer.seek(0)

        self.client.put_object(
            self.bucket_name,
            object_path,
            buffer,
            length=len(buffer.getvalue()),
            content_type=content_type
        )

        self.logger.info(f"Uploaded DataFrame to '{object_path}' as {format}")
        return object_path

    def _upload_dataframe_chunked(
        self,
        df: pd.DataFrame,
        object_path: str,
        format: str,
        chunk_size: int,
        compression: Optional[str] = None,
        **kwargs
    ) -> List[str]:
        """Upload DataFrame in chunks."""
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
        uploaded_paths = []

        base_path, ext = os.path.splitext(object_path)

        for i, chunk in enumerate(chunks):
            chunk_path = f"{base_path}_part_{i+1:04d}{ext}"
            self._upload_dataframe_single(chunk, chunk_path, format, compression, **kwargs)
            uploaded_paths.append(chunk_path)

        self.logger.info(f"Uploaded DataFrame in {len(chunks)} chunks to '{base_path}_part_*{ext}'")
        return uploaded_paths

    def list_objects(
        self,
        prefix: str = '',
        recursive: bool = True,
        bucket_name: Optional[str] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        List objects in bucket with optional prefix filtering.

        Args:
            prefix: Optional prefix to filter objects.
            recursive: Whether to list objects recursively.
            bucket_name: Optional bucket name (uses default if not specified).

        Yields:
            Dictionary containing object information.

        Raises:
            MinioOperationError: If listing fails.
        """
        bucket = bucket_name or self.bucket_name

        try:
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=recursive)

            for obj in objects:
                yield {
                    'object_name': obj.object_name,
                    'size': obj.size,
                    'etag': obj.etag,
                    'last_modified': obj.last_modified,
                    'content_type': getattr(obj, 'content_type', None),
                    'is_dir': obj.is_dir
                }

        except Exception as e:
            self.logger.error(f"Failed to list objects: {str(e)}")
            raise MinioOperationError(f"Failed to list objects: {str(e)}")

    def object_exists(self, object_path: str, bucket_name: Optional[str] = None) -> bool:
        """
        Check if an object exists in MinIO.

        Args:
            object_path: Path to the object.
            bucket_name: Optional bucket name (uses default if not specified).

        Returns:
            True if object exists, False otherwise.
        """
        bucket = bucket_name or self.bucket_name

        try:
            self.client.stat_object(bucket, object_path)
            return True
        except S3Error:
            return False
        except Exception as e:
            self.logger.error(f"Error checking object existence: {str(e)}")
            return False

    def get_object_info(self, object_path: str, bucket_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get detailed information about an object.

        Args:
            object_path: Path to the object.
            bucket_name: Optional bucket name (uses default if not specified).

        Returns:
            Dictionary containing object metadata.

        Raises:
            MinioOperationError: If getting object info fails.
        """
        bucket = bucket_name or self.bucket_name

        try:
            stat = self.client.stat_object(bucket, object_path)

            return {
                'object_name': stat.object_name,
                'size': stat.size,
                'etag': stat.etag,
                'last_modified': stat.last_modified,
                'content_type': stat.content_type,
                'metadata': stat.metadata,
                'version_id': getattr(stat, 'version_id', None)
            }

        except Exception as e:
            self.logger.error(f"Failed to get object info for '{object_path}': {str(e)}")
            raise MinioOperationError(f"Failed to get object info: {str(e)}")

    def copy_object(
        self,
        source_path: str,
        destination_path: str,
        source_bucket: Optional[str] = None,
        destination_bucket: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Copy an object within MinIO.

        Args:
            source_path: Source object path.
            destination_path: Destination object path.
            source_bucket: Source bucket (uses default if not specified).
            destination_bucket: Destination bucket (uses default if not specified).
            metadata: Optional new metadata for the copied object.

        Returns:
            Destination object path.

        Raises:
            MinioOperationError: If copy operation fails.
        """
        src_bucket = source_bucket or self.bucket_name
        dst_bucket = destination_bucket or self.bucket_name

        try:
            copy_source = CopySource(src_bucket, source_path)

            self.client.copy_object(
                dst_bucket,
                destination_path,
                copy_source,
                metadata=metadata
            )

            self.logger.info(f"Copied '{src_bucket}/{source_path}' to '{dst_bucket}/{destination_path}'")
            return destination_path

        except Exception as e:
            self.logger.error(f"Failed to copy object: {str(e)}")
            raise MinioOperationError(f"Failed to copy object: {str(e)}")

    def _get_content_type(self, file_extension: str) -> str:
        """Get content type based on file extension."""
        content_types = {
            '.txt': 'text/plain',
            '.csv': 'text/csv',
            '.json': 'application/json',
            '.xml': 'application/xml',
            '.html': 'text/html',
            '.css': 'text/css',
            '.js': 'application/javascript',
            '.pdf': 'application/pdf',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.gif': 'image/gif',
            '.zip': 'application/zip',
            '.tar': 'application/x-tar',
            '.gz': 'application/gzip',
            '.parquet': 'application/octet-stream',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.xls': 'application/vnd.ms-excel'
        }
        return content_types.get(file_extension.lower(), 'application/octet-stream')



# Example usage and testing
if __name__ == "__main__":
    # Example usage
    minio_config = dict(endpoint="minio-cdp-prod.apps.ocpdwhp.dwhmartr.bank",
            access_key='xvMjyTKmmjhdgjnhWwbr6Ha',
            secret_key='nhfghnhg',
            bucket_name='test',
            secure=True)


    try:
        with MinioHandler(minio_config) as handler:
            health = handler.health_check()
            print(f"Health status for bucket: {health}")
    except RuntimeError as e:
        print(f"Error during MinIO operation: {e}")

    # --- Use Case 2: Using without a context manager (manual connection) ---
    print("\n--- Testing without 'with' statement ---")
    handler = None
    try:
        handler = MinioHandler(minio_config)
        handler.connect()

        health = handler.health_check()
        print(f"Health status for bucket': {health}")

    except RuntimeError as e:
        print(f"Error during MinIO operation: {e}")
    finally:
        if handler:
