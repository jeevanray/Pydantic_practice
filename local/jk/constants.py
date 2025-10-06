"""
The file contains the list of all the constants
Version 1: 06th Jan 2025
Developer: HCL MARTECH
# pylint: disable
    C0301 --Line greated than 100 characters
    W0718 --General exception raised

"""

##Update this code as per the account
MAXTRIES = 3
TIME_ELAPSED = 30
TIMEOUT = 600
SFTP_WAITTIME = 1800
SFTP_RETRIES = 20
METADATAPATH = "/opt/airflow/etl_metadata/"
FILE_DATEFORMAT = "%d%m%Y"
FILETIMEFORMAT = "%d%m%Y_%H%M%S"
DATEFORMAT = "%d-%m-%Y"
DATETIMEFORMAT = "%d-%m-%Y %H:%M:%S"

METADATA = {
    "metadata_tables": [
        "configuration",
        "configuration_run_log",
    ],  ## Don't change the sequence will result in code failure
    "sys_config": "/opt/airflow/etl_metadata/sys_config.ini",
    "oracle_db": ['airflow_last_business_date', 'airflow_run_log']
}
PII = []
ORACLE_DATATYPE_MAP = {
    "NUMBER": "str",
    "DECIMAL": "str",
    "VARCHAR2": "str",
    "CHAR": "str",
    "CLOB": "str",
    "BLOB": "str",  # Or you can handle it differently
}
ORACLE_DATE = "YYYY-MM-DD"
ORACLE_TIMESTAMP = "YYYY-MM-DD HH24:MI:SS.FF"
FILEENCODIFNG = "ISO-8859-1"
ACKCOLUMNS = {"filename_col": "File_Name", "count_col": "Record_cnt"}
LOG_FORMAT = "%(filename)s:%(lineno)d - %(levelname)s- %(message)s"
NAN_VALUES = [ "", " ", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NULL", "NaN", "None", "n/a", "nan", "null "]
