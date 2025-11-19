# import json
import yaml
from audit_log import AuditLog
from source import SourceScanner
from processor import FileProcessor
from target import DBLoader

class Pipeline:
    def __init__(self, config):
        self.config = config
        self.source = SourceScanner(config)
        self.audit = AuditLog(
            config["audit"]
        )
        self.processor = FileProcessor(config["pipeline"]["columns"])
        self.loader = DBLoader(
            config["target"]
        )
        self.bucket = config["storage"]["bucket"]

    def run(self):
        latest_processed = self.audit.get_last_processed_dt()
        files = self.source.list_files()

        new_files = [
            f for f in files
            if f["last_modified"] >= latest_processed
        ]

        if not new_files:
            return

        for f in new_files:
            df = self.processor.process(
                self.source.client,
                self.bucket,
                f["file_name"]
            )

            segment_name = f["file_name"].split('/')[-2]

            df['last_modified_dt'] = f["last_modified"]
            df['segment_name'] = segment_name

            self.loader.insert_records(df)

            self.audit.log_file(
                f["file_name"],
                segment_name,
                f["last_modified"]
            )

if __name__ == "__main__":
    with open("segment_loader/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    Pipeline(config).run()
