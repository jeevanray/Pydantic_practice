import io
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_fixed
from minio import Minio
from minio.error import S3Error


class MinioHandler:
    """
    MinioHandler provides a wrapper for interacting with MinIO storage.
    Supports DataFrame uploads in parallel, with retries and context manager.
    """

    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = False, max_workers: int = 4):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.max_workers = max_workers

        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self._init_client()

    def _init_client(self):
        """Initialize MinIO client."""
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )
        self.logger.info("MinIO client initialized")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close connection (dummy, since MinIO client does not require explicit close)."""
        self.logger.info("Closing MinIO handler")

    def bucket_exists_or_create(self, bucket_name: str):
        """Ensure bucket exists, create if not."""
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
            self.logger.info(f"Bucket created: {bucket_name}")
        else:
            self.logger.info(f"Bucket already exists: {bucket_name}")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def upload_dataframe(self, bucket_name: str, object_name: str, df: pd.DataFrame):
        """
        Upload a single DataFrame to MinIO as parquet with retry.
        """
        try:
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=parquet_buffer,
                length=len(parquet_buffer.getvalue()),
                content_type="application/octet-stream",
            )
            self.logger.info(f"Uploaded {object_name} to {bucket_name}")
        except S3Error as e:
            self.logger.error(f"S3 error during upload {object_name}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during upload {object_name}: {e}")
            raise

    def upload_large_dataframe(self, bucket_name: str, base_object_name: str, df: pd.DataFrame, chunk_size: int = 100000):
        """
        Split large DataFrame into chunks and upload in parallel.
        """
        self.bucket_exists_or_create(bucket_name)

        chunks = [
            df.iloc[i:i + chunk_size]
            for i in range(0, len(df), chunk_size)
        ]

        futures = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for idx, chunk in enumerate(chunks):
                object_name = f"{base_object_name}_part{idx}.parquet"
                futures.append(executor.submit(self.upload_dataframe, bucket_name, object_name, chunk))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Upload failed: {e}")
                    raise

        self.logger.info(f"Completed parallel upload of {len(chunks)} chunks for {base_object_name}")
