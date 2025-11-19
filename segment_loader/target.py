import oracledb
from datetime import datetime

class DBLoader:
    def __init__(self, config):
        self.config = config
        self.conn = oracledb.connect(
            user=config["user"],
            password=config["password"],
            dsn=config["dsn"]
        )
        self._create_table_if_not_exists()
        self._create_constraint_if_not_exists()

    def _create_table_if_not_exists(self):
        ddl = """
        BEGIN
            EXECUTE IMMEDIATE '
            CREATE TABLE SEGMENT_EXPORT (
                CIF VARCHAR2(200),
                SEGMENT_NAME VARCHAR2(200),
                LAST_MODIFIED_DT DATE,
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

    def _create_constraint_if_not_exists(self):
        ddl = """
        BEGIN
            EXECUTE IMMEDIATE '
            ALTER TABLE SEGMENT_EXPORT
            ADD CONSTRAINT SEGMENT_EXPORT_PK
            UNIQUE (CIF, LAST_MODIFIED_DT)
            ';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -2261 THEN
                    RAISE;
                END IF;
        END;
        """
        with self.conn.cursor() as cur:
            cur.execute(ddl)
            self.conn.commit()

    def insert_records(self, df):
        insert_sql = """
            INSERT INTO SEGMENT_EXPORT
            (CIF, SEGMENT_NAME, LAST_MODIFIED_DT, CREATED_DT)
            VALUES (:1, :2, :3, :4)
        """

        records = [
            (
                r["cif"],
                r["segment_name"],
                r["last_modified_dt"],
                datetime.now()
            )
            for r in df.to_dict(orient='records')
        ]

        with self.conn.cursor() as cur:
            cur.executemany(insert_sql, records)
            self.conn.commit()
