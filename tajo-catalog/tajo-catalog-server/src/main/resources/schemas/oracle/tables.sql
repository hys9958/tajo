CREATE TABLE TABLES (
  TID NUMBER(10) NOT NULL PRIMARY KEY,
  DB_ID INT NOT NULL,
  TABLE_NAME VARCHAR2(128) NOT NULL,
  TABLE_TYPE VARCHAR2(128) NOT NULL,
  PATH VARCHAR2(4000),
  STORE_TYPE CHAR(16),
  FOREIGN KEY (DB_ID) REFERENCES DATABASES_ (DB_ID)
)