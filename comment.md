-- ====== 고정: 출력 경로(절대경로, 끝에 \ 또는 / 포함) ======
DEFINE OUTDIR = 'C:\ddl\';       -- Windows 예: C:\ddl\
-- DEFINE OUTDIR = '/home/you/ddl/';  -- Linux/macOS 예: /home/you/ddl/

-- ====== 실행 시 단 하나만 입력: OWNER ======
-- 예: SCOTT, HR 등

-- ====== 공통 세션 옵션 ======
SET PAGESIZE 0 LINESIZE 32767 LONG 100000 LONGCHUNKSIZE 100000 TRIMSPOOL ON
SET FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF TERMOUT OFF SERVEROUTPUT OFF

-- 타임스탬프는 내부 계산(질문/입력 없음)
COLUMN now_fmt NEW_VALUE now_fmt
SELECT TO_CHAR(SYSDATE, 'YYYYMMDD_HH24MISS') AS now_fmt FROM dual;

-- 파일명: ddl_with_missing_comments_<OWNER>_<YYYYMMDD_HH24MISS>.sql
SPOOL &OUTDIR.ddl_with_missing_comments_&OWNER._&&now_fmt..sql

-- DBMS_METADATA 출력 옵션(불필요 항목 제거, 세미콜론 포함)
BEGIN
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',false);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',false);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'REF_CONSTRAINTS',true);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS',true);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'OID',false);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',true);
END;
/

-- 1) 스키마 내 모든 테이블 DDL
SELECT DBMS_METADATA.GET_DDL('TABLE', t.table_name, UPPER('&OWNER'))
FROM   dba_tables t
WHERE  t.owner = UPPER('&OWNER')
ORDER  BY t.table_name;

-- 2) 테이블 코멘트 누락분 DDL
SELECT
  'COMMENT ON TABLE ' || t.owner || '.' || t.table_name ||
  ' IS ''' || REPLACE(INITCAP(REPLACE(LOWER(t.table_name),'_',' ')),'''','''''') || ''';'
FROM dba_tables t
LEFT JOIN dba_tab_comments tc
  ON tc.owner = t.owner AND tc.table_name = t.table_name
WHERE t.owner = UPPER('&OWNER')
  AND NVL(tc.comments,'') IS NULL
ORDER BY t.table_name;

-- 3) 컬럼 코멘트 누락분 DDL (숨김 컬럼 제외: DBA_TAB_COLS 사용)
SELECT
  'COMMENT ON COLUMN ' || c.owner || '.' || c.table_name || '.' || c.column_name ||
  ' IS ''' || REPLACE(INITCAP(REPLACE(LOWER(c.column_name),'_',' ')),'''','''''') || ''';'
FROM dba_tab_cols c
LEFT JOIN dba_col_comments cc
  ON cc.owner = c.owner AND cc.table_name = c.table_name AND cc.column_name = c.column_name
WHERE c.owner = UPPER('&OWNER')
  AND NVL(cc.comments,'') IS NULL
  AND NVL(c.hidden_column,'NO') = 'NO'
ORDER BY c.table_name, c.column_id;

SPOOL OFF

-- ====== 원래 표시 복구 ======
SET FEEDBACK ON VERIFY ON HEADING ON ECHO ON TERMOUT ON
