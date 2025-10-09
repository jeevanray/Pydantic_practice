ALTER SESSION SET CONTAINER=XEPDB1;

CREATE TABLE appuser.test_table (
  id NUMBER PRIMARY KEY,
  name VARCHAR2(100),
  created_date DATE
);

INSERT INTO appuser.test_table VALUES (1, 'Alice', SYSDATE);
INSERT INTO appuser.test_table VALUES (2, 'Bob', SYSDATE);
COMMIT;
