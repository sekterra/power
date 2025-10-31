-- 논리 요소: 각 테이블을 ArchiMate DataObject로 표현
-- id: 'DO:OWNER.TABLE_NAME'
-- name: (테이블 주석이 있으면) 주석 || 없으면 테이블명  + ' (Logical)'
-- documentation: 원물리명 표시 등 보조정보
SELECT
    'DO:' || t.owner || '.' || t.table_name                                           AS id,
    COALESCE(tc.comments, t.table_name) || ' (Logical)'                               AS name,
    'DataObject'                                                                       AS type,
    'Physical=' || t.owner || '.' || t.table_name                                      AS documentation
FROM all_tables t
LEFT JOIN all_tab_comments tc
       ON tc.owner = t.owner AND tc.table_name = t.table_name
WHERE t.owner IN ('HR','SCOTT')  -- TODO: 스키마 지정
ORDER BY t.owner, t.table_name;

--fk추정
-- 준비: PK 컬럼 목록
WITH pk_cols AS (
    SELECT c.owner, c.table_name, cc.column_name, cc.position
    FROM all_constraints c
    JOIN all_cons_columns cc
      ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
    WHERE c.constraint_type = 'P'
      AND c.owner IN ('HR','SCOTT')  -- TODO: 스키마 지정
),
single_pk AS (  -- 단일 PK 테이블
    SELECT owner, table_name, MAX(column_name) AS pk_col
    FROM pk_cols
    GROUP BY owner, table_name
    HAVING COUNT(*) = 1
),
composite_pk AS (  -- 복합 PK 테이블
    SELECT owner, table_name, COUNT(*) AS pk_cnt
    FROM pk_cols
    GROUP BY owner, table_name
    HAVING COUNT(*) > 1
),
-- 규칙 A: 단일 PK 컬럼명이 자식에 존재 + 타입 호환(간단화)
inf_assoc_single AS (
    SELECT DISTINCT
           child.owner        AS c_owner,
           child.table_name   AS c_table,
           spk.owner          AS p_owner,
           spk.table_name     AS p_table,
           spk.pk_col         AS match_col
    FROM single_pk spk
    JOIN all_tab_columns pcol
      ON pcol.owner = spk.owner
     AND pcol.table_name = spk.table_name
     AND pcol.column_name = spk.pk_col
    JOIN all_tab_columns child
      ON child.owner IN ('HR','SCOTT')   -- TODO: 스키마 지정(자식 후보)
     AND child.column_name = spk.pk_col
     AND (
           child.data_type = pcol.data_type
           OR (child.data_type = 'NUMBER' AND pcol.data_type = 'NUMBER')
         )
    WHERE NOT (child.owner = spk.owner AND child.table_name = spk.table_name)
),
-- 규칙 B: 복합 PK 모든 컬럼이 자식에 존재
inf_assoc_composite AS (
    SELECT
        c.owner      AS c_owner,
        c.table_name AS c_table,
        p.owner      AS p_owner,
        p.table_name AS p_table,
        COUNT(*)     AS hit_cnt
    FROM composite_pk p
    JOIN pk_cols pk
      ON pk.owner = p.owner AND pk.table_name = p.table_name
    JOIN all_tab_columns c
      ON c.owner IN ('HR','SCOTT')  -- TODO: 스키마 지정(자식 후보)
     AND c.column_name = pk.column_name
    GROUP BY c.owner, c.table_name, p.owner, p.table_name
    HAVING COUNT(*) = MAX(p.pk_cnt)
)
-- 논리 Association 결과 (단일/복합 PK 추정)
SELECT
    'ASSOC_LOG:' || s.c_owner || '.' || s.c_table || '->' || s.p_owner || '.' || s.p_table || ':S:' || s.match_col AS id,
    'Association'                                                                                                   AS type,
    'DO:' || s.c_owner || '.' || s.c_table                                                                           AS source,
    'DO:' || s.p_owner || '.' || s.p_table                                                                           AS target,
    'INFERRED (single PK) ON ' || s.match_col                                                                         AS name
FROM inf_assoc_single s

UNION ALL

SELECT
    'ASSOC_LOG:' || c.c_owner || '.' || c.c_table || '->' || c.p_owner || '.' || c.p_table || ':C'                    AS id,
    'Association'                                                                                                     AS type,
    'DO:' || c.c_owner || '.' || c.c_table                                                                            AS source,
    'DO:' || c.p_owner || '.' || c.p_table                                                                            AS target,
    'INFERRED (composite PK match)'                                                                                   AS name
FROM inf_assoc_composite c
ORDER BY 1;


-- 각 논리 테이블에 컬럼의 "논리 이름"을 속성으로 기록
-- key 예: column:EMAIL:logicalName , value = (주석 있으면 주석, 없으면 EMAIL)
SELECT
    'DO:' || c.owner || '.' || c.table_name                 AS elementId,
    'column:' || c.column_name || ':logicalName'            AS key,
    COALESCE(cc.comments, c.column_name)                    AS value
FROM all_tab_columns c
LEFT JOIN all_col_comments cc
       ON cc.owner = c.owner AND cc.table_name = c.table_name AND cc.column_name = c.column_name
WHERE c.owner IN ('HR','SCOTT')  -- TODO: 스키마 지정
ORDER BY c.owner, c.table_name, c.column_id;
