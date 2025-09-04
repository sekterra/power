-- 바인드 변수 예시
-- :owner      = 조회할 스키마 (예: 'HR')
-- :table_like = 테이블명 Like 필터(옵션, 전체면 NULL)

WITH tables AS (
  SELECT t.owner, t.table_name
  FROM   dba_tables t
  WHERE  t.owner = UPPER(:owner)
     AND (:table_like IS NULL OR t.table_name LIKE UPPER(:table_like))
),
cols AS (
  SELECT c.owner, c.table_name, c.column_name,
         c.data_type, c.data_length, c.char_used, c.char_length,
         c.data_precision, c.data_scale,
         c.nullable, c.data_default
  FROM   dba_tab_columns c
         JOIN tables t ON t.owner=c.owner AND t.table_name=c.table_name
),
pkc AS ( -- PK 컬럼 원본
  SELECT cc.owner, cc.table_name, cc.column_name, c.constraint_name
  FROM   dba_constraints c
         JOIN dba_cons_columns cc
           ON c.owner=cc.owner AND c.constraint_name=cc.constraint_name
         JOIN tables t ON t.owner=c.owner AND t.table_name=c.table_name
  WHERE  c.constraint_type='P'
),
pk_names AS ( -- PK 이름 집계
  SELECT owner, table_name, column_name,
         LISTAGG(constraint_name, ',') WITHIN GROUP (ORDER BY constraint_name) AS pk_names
  FROM   (SELECT DISTINCT owner, table_name, column_name, constraint_name FROM pkc)
  GROUP  BY owner, table_name, column_name
),
uqc AS ( -- UNIQUE 컬럼 원본
  SELECT DISTINCT cc.owner, cc.table_name, cc.column_name, c.constraint_name
  FROM   dba_constraints c
         JOIN dba_cons_columns cc
           ON c.owner=cc.owner AND c.constraint_name=cc.constraint_name
         JOIN tables t ON t.owner=c.owner AND t.table_name=c.table_name
  WHERE  c.constraint_type='U'
),
uq_names AS ( -- UNIQUE 이름 집계
  SELECT owner, table_name, column_name,
         LISTAGG(constraint_name, ',') WITHIN GROUP (ORDER BY constraint_name) AS unique_names
  FROM   uqc
  GROUP  BY owner, table_name, column_name
),
fk_info AS ( -- FK: 컬럼별로 참조 요약
  SELECT cc.owner, cc.table_name, cc.column_name, c.constraint_name,
         r.owner AS r_owner, r.table_name AS r_table_name,
         LISTAGG(rc.column_name, ',') WITHIN GROUP (ORDER BY rc.position) AS ref_columns
  FROM   dba_constraints c
         JOIN dba_cons_columns cc
           ON c.owner=cc.owner AND c.constraint_name=cc.constraint_name
         JOIN dba_constraints r
           ON r.owner=c.r_owner AND r.constraint_name=c.r_constraint_name
         JOIN dba_cons_columns rc
           ON rc.owner=r.owner AND rc.constraint_name=r.constraint_name
         JOIN tables t ON t.owner=c.owner AND t.table_name=c.table_name
  WHERE  c.constraint_type='R'
  GROUP  BY cc.owner, cc.table_name, cc.column_name, c.constraint_name, r.owner, r.table_name
),
fk_agg AS ( -- FK 이름→참조테이블(컬럼들) 문자열 집계 (컬럼 단위)
  SELECT owner, table_name, column_name,
         LISTAGG(constraint_name||'→'||r_owner||'.'||r_table_name||'('||ref_columns||')',
                 ' | ') WITHIN GROUP (ORDER BY constraint_name) AS fk_refs
  FROM   fk_info
  GROUP  BY owner, table_name, column_name
),
chk AS ( -- 테이블의 CHECK 제약
  SELECT c.owner, c.table_name, c.constraint_name, c.search_condition
  FROM   dba_constraints c
         JOIN tables t ON t.owner=c.owner AND t.table_name=c.table_name
  WHERE  c.constraint_type='C'
),
col_chk AS ( -- 컬럼명이 들어간 CHECK 식만 컬럼 단위로 묶기(근사치)
  SELECT col.owner, col.table_name, col.column_name,
         LISTAGG(ch.search_condition, ' || ')
           WITHIN GROUP (ORDER BY ch.constraint_name) AS check_conditions
  FROM   cols col
         JOIN chk ch
           ON ch.owner=col.owner AND ch.table_name=col.table_name
  WHERE  REGEXP_LIKE(ch.search_condition,
                     '(^|[^A-Z0-9_])'||col.column_name||'([^A-Z0-9_]|$)', 'i')
  GROUP  BY col.owner, col.table_name, col.column_name
)
SELECT
  col.owner,
  col.table_name,
  tc.comments                               AS table_comment,
  col.column_name,
  /* 가독성 있는 데이터타입 포맷 */
  CASE
    WHEN col.data_type IN ('CHAR','NCHAR','VARCHAR2','NVARCHAR2')
      THEN col.data_type||'('||col.char_length||' '||
           DECODE(col.char_used,'B','BYTE','C','CHAR',NULL)||')'
    WHEN col.data_type = 'NUMBER' AND col.data_precision IS NOT NULL AND col.data_scale IS NOT NULL
      THEN 'NUMBER('||col.data_precision||','||col.data_scale||')'
    WHEN col.data_type = 'NUMBER' AND col.data_precision IS NOT NULL AND col.data_scale IS NULL
      THEN 'NUMBER('||col.data_precision||')'
    ELSE col.data_type
  END                                        AS data_type,
  col.nullable,
  TRIM(BOTH ' ' FROM REPLACE(col.data_default, CHR(10),' ')) AS data_default,
  /* PK/UNIQUE 여부 + 이름 */
  CASE WHEN pk.pk_names IS NOT NULL THEN 'Y' ELSE 'N' END     AS is_pk,
  pk.pk_names                                                 AS pk_name,
  CASE WHEN uq.unique_names IS NOT NULL THEN 'Y' ELSE 'N' END AS is_unique,
  uq.unique_names                                             AS unique_names,
  /* FK 요약(컬럼 기준) */
  fk.fk_refs,
  /* CHECK (컬럼이 등장하는 식만) */
  cc.check_conditions                                         AS column_checks,
  /* 컬럼 코멘트 */
  ccom.comments                                               AS column_comment
FROM cols col
LEFT JOIN dba_tab_comments  tc
       ON tc.owner=col.owner AND tc.table_name=col.table_name
LEFT JOIN dba_col_comments  ccom
       ON ccom.owner=col.owner AND ccom.table_name=col.table_name AND ccom.column_name=col.column_name
LEFT JOIN pk_names pk
       ON pk.owner=col.owner AND pk.table_name=col.table_name AND pk.column_name=col.column_name
LEFT JOIN uq_names uq
       ON uq.owner=col.owner AND uq.table_name=col.table_name AND uq.column_name=col.column_name
LEFT JOIN fk_agg   fk
       ON fk.owner=col.owner AND fk.table_name=col.table_name AND fk.column_name=col.column_name
LEFT JOIN col_chk  cc
       ON cc.owner=col.owner AND cc.table_name=col.table_name AND cc.column_name=col.column_name
ORDER BY col.owner, col.table_name, col.column_name;
