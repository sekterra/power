-- 바인드: :owner = 스키마명, :table_like = '%패턴%' 또는 NULL
WITH c AS (  -- 대상 컬럼
  SELECT c.owner, c.table_name, c.column_name,
         c.data_type, c.data_length, c.char_used, c.char_length,
         c.data_precision, c.data_scale, c.nullable, c.data_default
  FROM   dba_tab_columns c
  WHERE  c.owner = UPPER(:owner)
    AND  (:table_like IS NULL OR c.table_name LIKE UPPER(:table_like))
),
tcom AS ( -- 테이블 코멘트
  SELECT owner, table_name, comments
  FROM   dba_tab_comments
  WHERE  owner = UPPER(:owner) AND table_type = 'TABLE'
),
ccm AS ( -- 컬럼 코멘트
  SELECT owner, table_name, column_name, comments
  FROM   dba_col_comments
  WHERE  owner = UPPER(:owner)
),
refcols AS ( -- 제약별 컬럼 목록(참조측 포함)
  SELECT owner, constraint_name,
         LISTAGG(column_name, ',') WITHIN GROUP(ORDER BY position) AS cols
  FROM   dba_cons_columns
  GROUP  BY owner, constraint_name
),
pk_agg AS ( -- PK: 컬럼 단위 집계
  SELECT cc.owner, cc.table_name, cc.column_name,
         LISTAGG(c.constraint_name, ',') WITHIN GROUP(ORDER BY c.constraint_name) AS pk_names
  FROM   dba_constraints c
  JOIN   dba_cons_columns cc
         ON c.owner=cc.owner AND c.constraint_name=cc.constraint_name
  WHERE  c.owner = UPPER(:owner)
    AND  c.constraint_type='P'
    AND  (:table_like IS NULL OR c.table_name LIKE UPPER(:table_like))
  GROUP  BY cc.owner, cc.table_name, cc.column_name
),
uq_agg AS ( -- UNIQUE: 컬럼 단위 집계
  SELECT cc.owner, cc.table_name, cc.column_name,
         LISTAGG(c.constraint_name, ',') WITHIN GROUP(ORDER BY c.constraint_name) AS uq_names
  FROM   dba_constraints c
  JOIN   dba_cons_columns cc
         ON c.owner=cc.owner AND c.constraint_name=cc.constraint_name
  WHERE  c.owner = UPPER(:owner)
    AND  c.constraint_type='U'
    AND  (:table_like IS NULL OR c.table_name LIKE UPPER(:table_like))
  GROUP  BY cc.owner, cc.table_name, cc.column_name
),
fk_agg AS ( -- FK: 컬럼 단위 요약 문자열
  SELECT cc.owner, cc.table_name, cc.column_name,
         LISTAGG(
           c.constraint_name || '→' || r.owner || '.' || r.table_name ||
           '(' || rc.cols || ')', ' | '
         ) WITHIN GROUP (ORDER BY c.constraint_name) AS fk_refs
  FROM   dba_constraints c
  JOIN   dba_cons_columns cc
         ON c.owner=cc.owner AND c.constraint_name=cc.constraint_name
  JOIN   dba_constraints r
         ON r.owner=c.r_owner AND r.constraint_name=c.r_constraint_name
  LEFT   JOIN refcols rc
         ON rc.owner=r.owner AND rc.constraint_name=r.constraint_name
  WHERE  c.owner = UPPER(:owner)
    AND  c.constraint_type='R'
    AND  (:table_like IS NULL OR c.table_name LIKE UPPER(:table_like))
  GROUP  BY cc.owner, cc.table_name, cc.column_name
),
ckname_agg AS ( -- CHECK: 식(LONG) 대신 이름만 컬럼 단위 집계
  SELECT cc.owner, cc.table_name, cc.column_name,
         LISTAGG(c.constraint_name, ',') WITHIN GROUP (ORDER BY c.constraint_name) AS check_names
  FROM   dba_constraints c
  JOIN   dba_cons_columns cc
         ON c.owner=cc.owner AND c.constraint_name=cc.constraint_name
  WHERE  c.owner = UPPER(:owner)
    AND  c.constraint_type='C'
    AND  (:table_like IS NULL OR c.table_name LIKE UPPER(:table_like))
  GROUP  BY cc.owner, cc.table_name, cc.column_name
)
SELECT
  c.owner,
  c.table_name,
  tcom.comments AS table_comment,
  c.column_name,
  CASE
    WHEN c.data_type IN ('CHAR','NCHAR','VARCHAR2','NVARCHAR2')
      THEN c.data_type || '(' || c.char_length || ' ' ||
           DECODE(c.char_used,'B','BYTE','C','CHAR',NULL) || ')'
    WHEN c.data_type = 'NUMBER' AND c.data_precision IS NOT NULL AND c.data_scale IS NOT NULL
      THEN 'NUMBER('||c.data_precision||','||c.data_scale||')'
    WHEN c.data_type = 'NUMBER' AND c.data_precision IS NOT NULL AND c.data_scale IS NULL
      THEN 'NUMBER('||c.data_precision||')'
    ELSE c.data_type
  END AS data_type,
  c.nullable,
  /* LONG 함수 금지 → DEFAULT는 가공 없이 그대로 노출 */
  c.data_default AS data_default,
  CASE WHEN pk.pk_names IS NOT NULL THEN 'Y' ELSE 'N' END AS is_pk,
  pk.pk_names AS pk_name,
  CASE WHEN uq.uq_names IS NOT NULL THEN 'Y' ELSE 'N' END AS is_unique,
  uq.uq_names AS unique_names,
  fk.fk_refs,
  ck.check_names AS check_names,       -- CHECK 제약 '이름' 목록(식은 ②에서 제공)
  ccm.comments AS column_comment
FROM c
LEFT JOIN tcom       ON tcom.owner=c.owner AND tcom.table_name=c.table_name
LEFT JOIN ccm        ON ccm.owner=c.owner AND ccm.table_name=c.table_name AND ccm.column_name=c.column_name
LEFT JOIN pk_agg pk  ON pk.owner=c.owner AND pk.table_name=c.table_name AND pk.column_name=c.column_name
LEFT JOIN uq_agg uq  ON uq.owner=c.owner AND uq.table_name=c.table_name AND uq.column_name=c.column_name
LEFT JOIN fk_agg fk  ON fk.owner=c.owner AND fk.table_name=c.table_name AND fk.column_name=c.column_name
LEFT JOIN ckname_agg ck ON ck.owner=c.owner AND ck.table_name=c.table_name AND ck.column_name=c.column_name
ORDER BY c.owner, c.table_name, c.column_name;
