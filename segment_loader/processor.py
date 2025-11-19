import pandas as pd
import os

class FileProcessor:
    def __init__(self, columns):
        self.columns = columns

    def process(self, bucket_client, bucket, key):
        local_path = f"{os.path.basename(key)}"
        bucket_client.download_file(bucket, key, local_path)
        df = pd.read_csv(local_path, dtype=str)
        if len(self.columns) == 1:
            return df.rename(columns={self.columns[0]: 'cif'})
        return df[self.columns]
