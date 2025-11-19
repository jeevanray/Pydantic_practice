import boto3

class SourceScanner:
    def __init__(self, config):
        storage = config["storage"]
        self.client = boto3.client(
            "s3",
            endpoint_url=storage["endpoint_url"],
            aws_access_key_id=storage["access_key"],
            aws_secret_access_key=storage["secret_key"]
        )
        self.bucket = storage["bucket"]
        self.folder = storage["folder"]

    def list_files(self):
        objects = self.client.list_objects_v2(Bucket=self.bucket, Prefix=self.folder).get("Contents", [])
        return [
            {
                "file_name": obj["Key"],
                "last_modified": obj["LastModified"]
            }
            for obj in objects
        ]

    def download(self, key, local_path):
        self.client.download_file(self.bucket, key, local_path)
########################
from minio import Minio
from datetime import datetime
import yaml

# class SourceScanner:
#     def __init__(self, config):
#         storage = config["storage"]
#         self.client = Minio(
#             storage["endpoint_url"].replace("https://", "").replace("http://", ""),
#             access_key=storage["access_key"],
#             secret_key=storage["secret_key"],
#             secure=storage.get("secure", True)  # default https
#         )
#         self.bucket = storage["bucket"]

#     def list_files(self):
#         objects = self.client.list_objects(self.bucket, recursive=True)
#         files = []
#         for obj in objects:
#             files.append({
#                 "file_name": obj.object_name,
#                 "last_modified": obj.last_modified
#             })
#         return files

#     def download(self, key, local_path):
#         self.client.fget_object(self.bucket, key, local_path)


if __name__ == "__main__":
    # Example usage
    with open("segment_loader/config.yaml", 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    scanner = SourceScanner(config)
    files = scanner.list_files()
    for f in files:
        print(f["file_name"], f["last_modified"])
