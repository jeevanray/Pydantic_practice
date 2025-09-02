import io
import json
import logging
import os
import threading
import time
import hashlib
import functools
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union, Callable
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


def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator to retry functions on failure with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_retries:
                        break
                    
                    # Log retry attempt
                    if hasattr(args[0], 'logger'):
                        args[0].logger.warning(
                            f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            # If we get here, all retries failed
            raise last_exception
        return wrapper
    return decorator


class MinioHandler(Minio):
    """
    Enhanced MinIO handler that inherits from Minio class and adds additional functionality.
    
    This class provides:
    - All native Minio methods through inheritance
    - MD5 calculation and metadata storage for uploads
    - Retry mechanisms for connections and operations
    - Multi-threading support for batch operations
    - Comprehensive logging and error handling
    - Full backward compatibility with existing code
    """
    
    def __init__(self, config: Dict[str, Any], max_workers: int = 4):
        """
        Initialize the enhanced MinioHandler.
        
        Args:
            config: Configuration dictionary with MinIO connection details
            max_workers: Maximum number of threads for parallel operations
        """
        # Validate required configuration
        required_keys = ['endpoint', 'access_key', 'secret_key']
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")
        
        self._config = config.copy()
        self.endpoint = self._config['endpoint']
        self.bucket_name = self._config.get('bucket_name', "sbi-test")
        self.max_workers = max_workers
        
        # Setup logging
        self.logger = self._setup_logging()
        
        # Thread safety
        self._lock = threading.RLock()
        self._bucket_exists_cache: Dict[str, bool] = {}
        self._is_connected = False
        
        # Initialize connection with retry
        self._initialize_connection()
    
    @retry_on_failure(max_retries=3, delay=1.0, backoff=2.0)
    def _initialize_connection(self):
        """Initialize MinIO connection with retry mechanism."""
        try:
            # Disable SSL verification warnings
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
            
            # Prepare Minio constructor arguments
            minio_kwargs = {
                'endpoint': self._config['endpoint'],
                'access_key': self._config['access_key'],
                'secret_key': self._config['secret_key'],
                'secure': self._config.get('secure', True),
                'http_client': http_client
            }
            
            if 'region' in self._config:
                minio_kwargs['region'] = self._config['region']
            
            # Initialize parent Minio class
            super().__init__(**minio_kwargs)
            
            # Validate connection
            self._validate_connection()
            
            # Ensure default bucket exists
            self._ensure_bucket_exists(self.bucket_name)
            
            self._is_connected = True
            self.logger.info(f"MinioHandler initialized successfully for endpoint: {self.endpoint}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize MinIO connection: {str(e)}")
            raise MinioConnectionError(f"Failed to connect to MinIO: {str(e)}")
    
    def __enter__(self):
        """Context manager entry."""
        if not self._is_connected:
            self._initialize_connection()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit."""
        self.close()
    
    def connect(self) -> None:
        """Maintain backward compatibility - connection is handled in __init__."""
        if not self._is_connected:
            self._initialize_connection()
    
    def close(self):
        """Close connection and cleanup resources."""
        with self._lock:
            if self._is_connected:
                self.logger.info("MinIO connection closed")
                self._is_connected = False
                self._bucket_exists_cache.clear()
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
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
        """Validate MinIO connection."""
        try:
            list(self.list_buckets())
            self.logger.debug("Connection validation successful")
        except Exception as e:
            raise MinioConnectionError(f"Connection validation failed: {str(e)}")
    
    def _ensure_bucket_exists(self, bucket_name: str) -> None:
        """Ensure bucket exists, create if necessary."""
        if bucket_name in self._bucket_exists_cache:
            return
        
        try:
            if not self.bucket_exists(bucket_name):
                self.make_bucket(bucket_name)
                self.logger.info(f"Created bucket '{bucket_name}'")
            self._bucket_exists_cache[bucket_name] = True
        except Exception as e:
            raise MinioOperationError(f"Failed to ensure bucket exists: {str(e)}")
    
    @staticmethod
    def _calculate_md5(data: bytes) -> str:
        """Calculate MD5 hash of data."""
        return hashlib.md5(data).hexdigest()
    
    @staticmethod
    def _calculate_file_md5(file_path: str, chunk_size: int = 8192) -> str:
        """Calculate MD5 hash of file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def health_check(self) -> Dict[str, Any]:
        """
        Comprehensive health check for MinIO connection.
        
        Returns:
            Dictionary containing health status information
        """
        try:
            start_time = time.time()
            
            if not self._is_connected:
                return {
                    "status": "unhealthy",
                    "error": "Not connected to MinIO",
                    "timestamp": time.time()
                }
            
            # Test operations
            buckets = list(self.list_buckets())
            bucket_accessible = self.bucket_exists(self.bucket_name)
            objects = list(self.list_objects(self.bucket_name, recursive=False))
            
            response_time = time.time() - start_time
            
            return {
                "status": "healthy",
                "endpoint": self.endpoint,
                "bucket_name": self.bucket_name,
                "bucket_accessible": bucket_accessible,
                "total_buckets": len(buckets),
                "object_count": len(objects),
                "response_time_ms": round(response_time * 1000, 2),
                "timestamp": time.time()
            }
            
        except Exception as e:
            self.logger.error(f"Health check failed: {str(e)}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "endpoint": self.endpoint,
                "timestamp": time.time()
            }
    
    @retry_on_failure(max_retries=3, delay=1.0, backoff=2.0)
    def read_file(
        self,
        object_path: str,
        return_type: str = 'bytes',
        bucket_name: Optional[str] = None
    ) -> Union[bytes, str, Dict[str, Any], pd.DataFrame]:
        """
        Read file content from MinIO with enhanced error handling.
        
        Args:
            object_path: Path to object in MinIO
            return_type: Return format ('bytes', 'string', 'json', 'dataframe')
            bucket_name: Optional bucket name
            
        Returns:
            File content in specified format
        """
        bucket = bucket_name or self.bucket_name
        
        try:
            self.logger.debug(f"Reading file '{object_path}' from bucket '{bucket}'")
            
            response = self.get_object(bucket, object_path)
            data = response.read()
            response.close()
            response.release_conn()
            
            self.logger.info(f"Successfully read file '{object_path}' ({len(data)} bytes)")
            
            if return_type == 'bytes':
                return data
            elif return_type == 'string':
                return data.decode('utf-8')
            elif return_type == 'json':
                return json.loads(data.decode('utf-8'))
            elif return_type == 'dataframe':
                ext = Path(object_path).suffix.lower()
                if ext == '.csv':
                    return pd.read_csv(io.BytesIO(data))
                elif ext in ['.parquet', '.pq']:
                    return pd.read_parquet(io.BytesIO(data))
                elif ext in ['.xlsx', '.xls']:
                    return pd.read_excel(io.BytesIO(data))
                else:
                    raise MinioOperationError(f"Unsupported file format: {ext}")
            else:
                raise ValueError(f"Unsupported return_type: {return_type}")
                
        except Exception as e:
            self.logger.error(f"Failed to read file '{object_path}': {str(e)}")
            raise MinioOperationError(f"Failed to read file: {str(e)}")
    
    @retry_on_failure(max_retries=3, delay=1.0, backoff=2.0)
    def download_file(
        self,
        object_path: str,
        local_path: str,
        overwrite: bool = False,
        progress_callback: Optional[Callable] = None,
        bucket_name: Optional[str] = None
    ) -> str:
        """
        Download file from MinIO with progress tracking and retry.
        
        Args:
            object_path: Path to object in MinIO
            local_path: Local destination path
            overwrite: Whether to overwrite existing files
            progress_callback: Optional progress callback function
            bucket_name: Optional bucket name
            
        Returns:
            Absolute path to downloaded file
        """
        bucket = bucket_name or self.bucket_name
        local_path_obj = Path(local_path)
        
        if local_path_obj.exists() and not overwrite:
            raise FileExistsError(f"Local file '{local_path}' already exists")
        
        local_path_obj.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            self.logger.debug(f"Downloading '{object_path}' to '{local_path}'")
            
            if progress_callback:
                # Download with progress tracking
                response = self.get_object(bucket, object_path)
                obj_info = self.stat_object(bucket, object_path)
                total_size = obj_info.size
                downloaded = 0
                
                with open(local_path, 'wb') as f:
                    while True:
                        chunk = response.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        progress_callback(downloaded, total_size)
                
                response.close()
                response.release_conn()
            else:
                self.fget_object(bucket, object_path, local_path)
            
            self.logger.info(f"Successfully downloaded '{object_path}' to '{local_path}'")
            return str(local_path_obj.absolute())
            
        except Exception as e:
            self.logger.error(f"Failed to download '{object_path}': {str(e)}")
            raise MinioOperationError(f"Failed to download file: {str(e)}")
    
    def delete_file(self, object_path: str, bucket_name: Optional[str] = None) -> bool:
        """Delete file from MinIO."""
        bucket = bucket_name or self.bucket_name
        
        try:
            self.logger.debug(f"Deleting file '{object_path}' from bucket '{bucket}'")
            self.remove_object(bucket, object_path)
            self.logger.info(f"Successfully deleted file '{object_path}'")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete file '{object_path}': {str(e)}")
            raise MinioOperationError(f"Failed to delete file: {str(e)}")
    
    def delete_folder(
        self,
        folder_path: str,
        bucket_name: Optional[str] = None,
        batch_size: int = 1000,
        use_threading: bool = True
    ) -> int:
        """
        Delete folder and contents with optional threading support.
        
        Args:
            folder_path: Path to folder in MinIO
            bucket_name: Optional bucket name
            batch_size: Objects per batch
            use_threading: Whether to use threading for deletion
            
        Returns:
            Number of objects deleted
        """
        bucket = bucket_name or self.bucket_name
        folder_path = folder_path.rstrip('/') + '/'
        deleted_count = 0
        
        try:
            self.logger.debug(f"Deleting folder '{folder_path}' from bucket '{bucket}'")
            
            # Get all objects
            objects = list(self.list_objects(bucket, prefix=folder_path, recursive=True))
            
            if use_threading and len(objects) > batch_size:
                deleted_count = self._delete_objects_threaded(bucket, objects, batch_size)
            else:
                deleted_count = self._delete_objects_batch(bucket, objects, batch_size)
            
            self.logger.info(f"Successfully deleted folder '{folder_path}' ({deleted_count} objects)")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Failed to delete folder '{folder_path}': {str(e)}")
            raise MinioOperationError(f"Failed to delete folder: {str(e)}")
    
    def _delete_objects_batch(self, bucket: str, objects: List, batch_size: int) -> int:
        """Delete objects in batches."""
        deleted_count = 0
        batch = []
        
        for obj in objects:
            batch.append(DeleteObject(obj.object_name))
            
            if len(batch) >= batch_size:
                errors = list(self.remove_objects(bucket, batch))
                deleted_count += len(batch) - len(errors)
                
                for error in errors:
                    self.logger.error(f"Failed to delete {error.object_name}: {error}")
                
                batch = []
        
        # Delete remaining objects
        if batch:
            errors = list(self.remove_objects(bucket, batch))
            deleted_count += len(batch) - len(errors)
        
        return deleted_count
    
    def _delete_objects_threaded(self, bucket: str, objects: List, batch_size: int) -> int:
        """Delete objects using threading for better performance."""
        deleted_count = 0
        batches = [objects[i:i + batch_size] for i in range(0, len(objects), batch_size)]
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._delete_objects_batch, bucket, batch, batch_size): batch
                for batch in batches
            }
            
            for future in as_completed(futures):
                try:
                    deleted_count += future.result()
                except Exception as e:
                    self.logger.error(f"Batch deletion failed: {str(e)}")
        
        return deleted_count
    
    @retry_on_failure(max_retries=3, delay=1.0, backoff=2.0)
    def upload_file(
        self,
        local_path: str,
        object_path: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        content_type: Optional[str] = None,
        bucket_name: Optional[str] = None,
        calculate_md5: bool = True
    ) -> str:
        """
        Upload file to MinIO with MD5 calculation and metadata storage.
        
        Args:
            local_path: Path to local file
            object_path: Destination path in MinIO
            metadata: Optional metadata dictionary
            content_type: Optional content type
            bucket_name: Optional bucket name
            calculate_md5: Whether to calculate and store MD5 hash
            
        Returns:
            Object path in MinIO
        """
        bucket = bucket_name or self.bucket_name
        local_file = Path(local_path)
        
        if not local_file.exists():
            raise FileNotFoundError(f"Local file '{local_path}' not found")
        
        if not object_path:
            object_path = local_file.name
        
        try:
            self.logger.debug(f"Uploading '{local_path}' to '{object_path}'")
            
            # Prepare metadata
            upload_metadata = metadata.copy() if metadata else {}
            
            # Calculate MD5 if requested
            if calculate_md5:
                file_md5 = self._calculate_file_md5(local_path)
                upload_metadata['md5sum'] = file_md5
                upload_metadata['upload_time'] = str(int(time.time()))
                self.logger.debug(f"Calculated MD5 for '{local_path}': {file_md5}")
            
            # Auto-detect content type
            if not content_type:
                content_type = self._get_content_type(local_file.suffix)
            
            # Upload file
            self.fput_object(
                bucket,
                object_path,
                str(local_file),
                content_type=content_type,
                metadata=upload_metadata
            )
            
            self.logger.info(f"Successfully uploaded '{local_path}' to '{object_path}'")
            return object_path
            
        except Exception as e:
            self.logger.error(f"Failed to upload '{local_path}': {str(e)}")
            raise MinioOperationError(f"Failed to upload file: {str(e)}")
    
    @retry_on_failure(max_retries=3, delay=1.0, backoff=2.0)
    def upload_large_file(
        self,
        local_path: str,
        object_path: Optional[str] = None,
        part_size: int = 10 * 1024 * 1024,
        progress_callback: Optional[Callable] = None,
        metadata: Optional[Dict[str, str]] = None,
        bucket_name: Optional[str] = None,
        calculate_md5: bool = True
    ) -> str:
        """
        Upload large file with multipart upload, MD5 calculation and progress tracking.
        
        Args:
            local_path: Path to local file
            object_path: Destination path in MinIO
            part_size: Size of each part in bytes
            progress_callback: Optional progress callback
            metadata: Optional metadata dictionary
            bucket_name: Optional bucket name
            calculate_md5: Whether to calculate and store MD5
            
        Returns:
            Object path in MinIO
        """
        bucket = bucket_name or self.bucket_name
        local_file = Path(local_path)
        
        if not local_file.exists():
            raise FileNotFoundError(f"Local file '{local_path}' not found")
        
        if not object_path:
            object_path = local_file.name
        
        try:
            file_size = local_file.stat().st_size
            self.logger.debug(f"Uploading large file '{local_path}' ({file_size} bytes)")
            
            # Prepare metadata
            upload_metadata = metadata.copy() if metadata else {}
            
            # Calculate MD5 if requested
            if calculate_md5:
                file_md5 = self._calculate_file_md5(local_path)
                upload_metadata['md5sum'] = file_md5
                upload_metadata['upload_time'] = str(int(time.time()))
                upload_metadata['file_size'] = str(file_size)
                self.logger.debug(f"Calculated MD5 for large file '{local_path}': {file_md5}")
            
            # Progress wrapper
            if progress_callback:
                class ProgressWrapper:
                    def __init__(self, file_path: str, callback: Callable, total_size: int):
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
                
                result = self.put_object(
                    bucket,
                    object_path,
                    file_obj,
                    length=file_size,
                    content_type=content_type,
                    metadata=upload_metadata,
                    part_size=part_size
                )
                
                self.logger.info(
                    f"Successfully uploaded large file '{local_path}' to '{object_path}' "
                    f"(ETag: {result.etag})"
                )
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
        bucket_name: Optional[str] = None,
        calculate_md5: bool = True,
        use_threading: bool = False,
        **kwargs
    ) -> Union[str, List[str]]:
        """
        Upload DataFrame to MinIO with MD5 calculation and optional chunking/threading.
        
        Args:
            df: DataFrame to upload
            object_path: Destination path
            format: File format ('csv', 'parquet', 'json', 'excel')
            chunk_size: Optional chunk size for large DataFrames
            compression: Optional compression
            bucket_name: Optional bucket name
            calculate_md5: Whether to calculate MD5
            use_threading: Whether to use threading for chunked uploads
            **kwargs: Additional pandas arguments
            
        Returns:
            Object path(s) in MinIO
        """
        bucket = bucket_name or self.bucket_name
        
        try:
            self.logger.debug(f"Uploading DataFrame ({len(df)} rows) to '{object_path}' as {format}")
            
            if chunk_size and len(df) > chunk_size:
                if use_threading:
                    return self._upload_dataframe_chunked_threaded(
                        df, object_path, format, chunk_size, compression, 
                        bucket, calculate_md5, **kwargs
                    )
                else:
                    return self._upload_dataframe_chunked(
                        df, object_path, format, chunk_size, compression,
                        bucket, calculate_md5, **kwargs
                    )
            else:
                return self._upload_dataframe_single(
                    df, object_path, format, compression, bucket, calculate_md5, **kwargs
                )
                
        except Exception as e:
            self.logger.error(f"Failed to upload DataFrame: {str(e)}")
            raise MinioOperationError(f"Failed to upload DataFrame: {str(e)}")
    
    def _upload_dataframe_single(
        self,
        df: pd.DataFrame,
        object_path: str,
        format: str,
        compression: Optional[str],
        bucket: str,
        calculate_md5: bool = True,
        **kwargs
    ) -> str:
        """Upload DataFrame as single file with MD5 calculation."""
        buffer = io.BytesIO()
        
        # Format-specific serialization
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
        data = buffer.getvalue()
        
        # Prepare metadata
        metadata = {
            'format': format,
            'rows': str(len(df)),
            'columns': str(len(df.columns)),
            'upload_time': str(int(time.time()))
        }
        
        if calculate_md5:
            md5_hash = self._calculate_md5(data)
            metadata['md5sum'] = md5_hash
            self.logger.debug(f"Calculated MD5 for DataFrame: {md5_hash}")
        
        # Upload
        buffer.seek(0)
        self.put_object(
            bucket,
            object_path,
            buffer,
            length=len(data),
            content_type=content_type,
            metadata=metadata
        )
        
        self.logger.info(f"Successfully uploaded DataFrame to '{object_path}' as {format}")
        return object_path
    
    def _upload_dataframe_chunked(
        self,
        df: pd.DataFrame,
        object_path: str,
        format: str,
        chunk_size: int,
        compression: Optional[str],
        bucket: str,
        calculate_md5: bool = True,
        **kwargs
    ) -> List[str]:
        """Upload DataFrame in chunks sequentially."""
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
        uploaded_paths = []
        base_path, ext = os.path.splitext(object_path)
        
        for i, chunk in enumerate(chunks):
            chunk_path = f"{base_path}_part_{i+1:04d}{ext}"
            self._upload_dataframe_single(
                chunk, chunk_path, format, compression, bucket, calculate_md5, **kwargs
            )
            uploaded_paths.append(chunk_path)
        
        self.logger.info(f"Uploaded DataFrame in {len(chunks)} chunks")
        return uploaded_paths
    
    def _upload_dataframe_chunked_threaded(
        self,
        df: pd.DataFrame,
        object_path: str,
        format: str,
        chunk_size: int,
        compression: Optional[str],
        bucket: str,
        calculate_md5: bool = True,
        **kwargs
    ) -> List[str]:
        """Upload DataFrame chunks using threading."""
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
        uploaded_paths = []
        base_path, ext = os.path.splitext(object_path)
        
        def upload_chunk(chunk_data, chunk_index):
            chunk_path = f"{base_path}_part_{chunk_index+1:04d}{ext}"
            self._upload_dataframe_single(
                chunk_data, chunk_path, format, compression, bucket, calculate_md5, **kwargs
            )
            return chunk_path
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(upload_chunk, chunk, i): i 
                for i, chunk in enumerate(chunks)
            }
            
            # Collect results in order
            results = [None] * len(chunks)
            for future in as_completed(futures):
                chunk_index = futures[future]
                try:
                    results[chunk_index] = future.result()
                except Exception as e:
                    self.logger.error(f"Chunk upload failed: {str(e)}")
                    raise
            
            uploaded_paths = [path for path in results if path is not None]
        
        self.logger.info(f"Uploaded DataFrame in {len(chunks)} chunks using threading")
        return uploaded_paths
    
    def list_objects(
        self,
        prefix: str = '',
        recursive: bool = True,
        bucket_name: Optional[str] = None,
        include_metadata: bool = False
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Enhanced object listing with optional metadata inclusion.
        
        Args:
            prefix: Optional prefix filter
            recursive: Whether to list recursively
            bucket_name: Optional bucket name
            include_metadata: Whether to include object metadata
            
        Yields:
            Dictionary containing object information
        """
        bucket = bucket_name or self.bucket_name
        
        try:
            self.logger.debug(f"Listing objects in bucket '{bucket}' with prefix '{prefix}'")
            objects = self.list_objects_v2(bucket, prefix=prefix, recursive=recursive)
            
            count = 0
            for obj in objects:
                count += 1
                obj_info = {
                    'object_name': obj.object_name,
                    'size': obj.size,
                    'etag': obj.etag,
                    'last_modified': obj.last_modified,
                    'content_type': getattr(obj, 'content_type', None),
                    'is_dir': obj.is_dir
                }
                
                # Include metadata if requested
                if include_metadata and not obj.is_dir:
                    try:
                        stat = self.stat_object(bucket, obj.object_name)
                        obj_info['metadata'] = stat.metadata
                    except Exception as e:
                        self.logger.debug(f"Failed to get metadata for {obj.object_name}: {e}")
                        obj_info['metadata'] = {}
                
                yield obj_info
            
            self.logger.debug(f"Listed {count} objects")
            
        except Exception as e:
            self.logger.error(f"Failed to list objects: {str(e)}")
            raise MinioOperationError(f"Failed to list objects: {str(e)}")
    
    def object_exists(self, object_path: str, bucket_name: Optional[str] = None) -> bool:
        """Check if object exists in MinIO."""
        bucket = bucket_name or self.bucket_name
        
        try:
            self.stat_object(bucket, object_path)
            return True
        except S3Error:
            return False
        except Exception as e:
            self.logger.error(f"Error checking object existence: {str(e)}")
            return False
    
    def get_object_info(self, object_path: str, bucket_name: Optional[str] = None) -> Dict[str, Any]:
        """Get detailed object information including metadata."""
        bucket = bucket_name or self.bucket_name
        
        try:
            self.logger.debug(f"Getting info for object '{object_path}'")
            stat = self.stat_object(bucket, object_path)
            
            info = {
                'object_name': stat.object_name,
                'size': stat.size,
                'etag': stat.etag,
                'last_modified': stat.last_modified,
                'content_type': stat.content_type,
                'metadata': stat.metadata,
                'version_id': getattr(stat, 'version_id', None)
            }
            
            self.logger.debug(f"Retrieved info for '{object_path}' ({stat.size} bytes)")
            return info
            
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
        """Copy object within MinIO."""
        src_bucket = source_bucket or self.bucket_name
        dst_bucket = destination_bucket or self.bucket_name
        
        try:
            self.logger.debug(f"Copying '{src_bucket}/{source_path}' to '{dst_bucket}/{destination_path}'")
            
            copy_source = CopySource(src_bucket, source_path)
            self.copy_object_v2(
                dst_bucket,
                destination_path,
                copy_source,
                metadata=metadata
            )
            
            self.logger.info(f"Successfully copied object to '{destination_path}'")
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
    minio_config = {
        'endpoint': "minio-cdp-prod.apps.ocpdwhp.dwhmartr.bank",
        'access_key': 'xvMjyTKmmjhdgjnhWwbr6Ha',
        'secret_key': 'nhfghnhg',
        'bucket_name': 'test',
        'secure': True
    }
    
    try:
        # Context manager usage
        with MinioHandler(minio_config, max_workers=4) as handler:
            health = handler.health_check()
            print(f"Health status: {health}")
            
            # Test native MinIO methods (inherited)
            buckets = list(handler.list_buckets())
            print(f"Available buckets: {len(buckets)}")
            
    except Exception as e:
        print(f"Error: {e}")
    
    # Manual connection usage
    try:
        handler = MinioHandler(minio_config, max_workers=4)
        health = handler.health_check()
        print(f"Health status: {health}")
        
        # All native MinIO methods are available
        # handler.make_bucket("new-bucket")  # Native MinIO method
        # handler.list_objects("test")       # Native MinIO method
        
        handler.close()
        
    except Exception as e:
        print(f"Error: {e}")
