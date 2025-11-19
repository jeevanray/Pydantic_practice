import oracledb
from datetime import datetime, timezone

class AuditLog:
    def __init__(self, config):
        self.conn = oracledb.connect(
            user=config["user"],
            password=config["password"],
            dsn=config["dsn"]
        )
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        ddl = """
        BEGIN
            EXECUTE IMMEDIATE '
            CREATE TABLE FILE_AUDIT (
                FILE_PATH VARCHAR2(200),
                SEGMENT_NAME VARCHAR2(200),
                LAST_MODIFIED_DT TIMESTAMP WITH TIME ZONE,
                PROCESSED_STATUS VARCHAR2(20),
                CREATED_DT DATE DEFAULT SYSDATE
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN 
                    RAISE;
                END IF;
        END;
        """
        with self.conn.cursor() as cur:
            cur.execute(ddl)
            self.conn.commit()

    def get_last_processed_dt(self):
        query = "SELECT MAX(LAST_MODIFIED_DT) FROM FILE_AUDIT"
        with self.conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()[0]
            if result is None:
                return datetime(1970, 1, 1, tzinfo=timezone.utc)
            return result

    def log_file(self, file_path, segment_name,last_modified):
        insert_sql = """
        INSERT INTO FILE_AUDIT (FILE_PATH, SEGMENT_NAME,LAST_MODIFIED_DT, PROCESSED_STATUS)
        VALUES (:1, :2, :3,'PROCESSED')
        """
        with self.conn.cursor() as cur:
            cur.execute(insert_sql, [file_path, segment_name,last_modified])
            self.conn.commit()
