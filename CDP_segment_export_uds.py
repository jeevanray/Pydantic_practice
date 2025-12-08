from segment_exporter.audit_log import AuditLog
from segment_exporter.source import SourceScanner
from segment_exporter.processor import FileProcessor
from segment_exporter.target import DBLoader
from minio_helper.logconfig import get_logger
from minio_helper.utilities import get_job_config, get_system_config
# import pytz
 
logger = get_logger(__name__)
 
# IST = pytz.timezone('Asia/Kolkata')
 
class Pipeline:
    def __init__(self, config):
        self.config = config
        self.source = SourceScanner(config)
        self.audit = AuditLog(
            config
        )
        self.processor = FileProcessor(config["pipeline"]["columns"])
        self.loader = DBLoader(
            config
        )
        self.bucket = config["storage"]["bucket"]
        self.folder_path = config["storage"].get("scan_folder")
 
 
    def run(self):
        latest_processed = self.audit.get_last_processed_dt()
        files = self.source.list_files(self.folder_path)
        processed_files = self.audit.get_processed_files_with_max_modified_dt()
        processed_set = set(x['file_name'] for x in processed_files)
        # print(f"processed_set{processed_set}")
        logger.info(f"latest_processed_dt: {latest_processed}")
        new_files = [
            f for f in files
            if f["last_modified"].replace(tzinfo=None) >= latest_processed.replace(tzinfo=None)
        ]
        new_lst = []
        for f in new_files:
            if f['file_name'] in processed_set:
                if f["last_modified"].replace(microsecond=0).replace(tzinfo=None) == latest_processed.replace(tzinfo=None):
                        pass
                else:
                    new_lst.append(f)
            else:
                new_lst.append(f)
        new_files = new_lst
        logger.info(f"new_list_after_filter: {new_files}")
        if not new_files:
            return
        for f in new_files:
            df = self.processor.process(
                self.source,
                self.bucket,
                f["file_name"]
            )
            segment_name = f["file_name"].split('/')[-2]
 
            df['last_modified_dt'] = f["last_modified"]
            df['segment_name'] = segment_name
            logger.info(f"segment_name : {segment_name}")
            try:
                self.loader.insert_records(df)
                self.audit.log_file(
                    f["file_name"],
                    segment_name,
                    f["last_modified"]
                )
            except Exception as e:
                logger.error(e)
 
if __name__ == "__main__":
    config_path = "etl_configs/segment_export_config.yaml"
    configurations = get_system_config()
    configurations.update(get_job_config(config_path))
    Pipeline(configurations).run()