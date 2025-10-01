-- ====== 파라미터 ======
-- 실행 시 &OWNER 에 스키마명 입력 (예: SCOTT)
-- 결과 파일명: ddl_with_missing_comments_&OWNER..sql
SET PAGESIZE 0 LINESIZE 32767 LONG 100000 LONGCHUNKSIZE 100000 TRIMSPOOL ON
SET FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF TERMOUT OFF SERVEROUTPUT OFF

COL now_fmt NEW_VALUE now_fmt
SELECT TO_CHAR(SYSDATE, 'YYYYMMDD_HH24MISS') AS now_fmt FROM dual;

SPOOL ddl_with_missing_comments_&OWNER._&&now_fmt..sql

-- 0) DBMS_METADATA 출력 옵션 (불필요한 STORAGE 등 제거, 세미콜론 찍기)
BEGIN
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',false);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'REF_CONSTRAINTS',true);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS',true);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'OID',false);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',true);
END;
/
-- 1) 테이블 DDL
SELECT DBMS_METADATA.GET_DDL('TABLE', t.table_name, UPPER('&OWNER'))
FROM   dba_tables t
WHERE  t.owner = UPPER('&OWNER')
ORDER  BY t.table_name;

-- 2) 테이블 코멘트 누락분 DDL
SELECT
  'COMMENT ON TABLE ' || tc.owner || '.' || tc.table_name ||
  ' IS ''' || REPLACE(INITCAP(REPLACE(LOWER(tc.table_name),'_',' ')),'''','''''') || ''';'
FROM dba_tables tc
LEFT JOIN dba_tab_comments cmt
  ON cmt.owner = tc.owner AND cmt.table_name = tc.table_name
WHERE tc.owner = UPPER('&OWNER')
  AND NVL(cmt.comments,'') IS NULL
ORDER BY tc.table_name;

-- 3) 컬럼 코멘트 누락분 DDL (숨김 컬럼 제외)
SELECT
  'COMMENT ON COLUMN ' || c.owner || '.' || c.table_name || '.' || c.column_name ||
  ' IS ''' || REPLACE(INITCAP(REPLACE(LOWER(c.column_name),'_',' ')),'''','''''') || ''';'
FROM dba_tab_columns c
LEFT JOIN dba_col_comments cc
  ON cc.owner = c.owner AND cc.table_name = c.table_name AND cc.column_name = c.column_name
WHERE c.owner = UPPER('&OWNER')
  AND NVL(cc.comments,'') IS NULL
  AND NVL(c.hidden_column,'NO') = 'NO'
ORDER BY c.table_name, c.column_id;

SPOOL OFF
SET FEEDBACK ON VERIFY ON HEADING ON ECHO ON TERMOUT ON
