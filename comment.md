-- 스키마 내에서 컬럼명별로 가장 많이 쓰인 주석을 표준으로 채택
WITH col_comment_stats AS (
  SELECT
      cc.column_name,
      cc.comments,
      COUNT(*) AS cnt,
      ROW_NUMBER() OVER (
        PARTITION BY cc.column_name
        ORDER BY COUNT(*) DESC, MIN(cc.table_name) ASC, MIN(cc.owner) ASC
      ) AS rn
  FROM   dba_col_comments cc
  WHERE  cc.owner = :OWNER
    AND  cc.comments IS NOT NULL
  GROUP  BY cc.column_name, cc.comments
)
SELECT
    column_name,
    comments AS standard_comment,
    cnt AS sample_count
FROM col_comment_stats
WHERE rn = 1
ORDER BY column_name;

WITH dict AS (
  -- 표준 주석 사전
  SELECT column_name, comments, cnt, rn
  FROM (
    SELECT
        cc.column_name,
        cc.comments,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (
          PARTITION BY cc.column_name
          ORDER BY COUNT(*) DESC, MIN(cc.table_name) ASC
        ) AS rn
    FROM   dba_col_comments cc
    WHERE  cc.owner = :OWNER
      AND  cc.comments IS NOT NULL
    GROUP  BY cc.column_name, cc.comments
  )
  WHERE rn = 1
)
-- 1) 이미 존재하는 테이블 주석 그대로 DDL
SELECT
  'COMMENT ON TABLE ' || t.owner || '.' || t.table_name ||
  ' IS q''[' || REPLACE(tc.comments, '''', '''''') || ']'';' AS ddl
FROM dba_tables t
JOIN dba_tab_comments tc
  ON tc.owner = t.owner AND tc.table_name = t.table_name
WHERE t.owner = :OWNER
  AND tc.comments IS NOT NULL
UNION ALL
-- 2) 테이블 주석이 없을 때, 컬럼명 = 테이블명 인 표준 주석으로 대체 생성
SELECT
  'COMMENT ON TABLE ' || t.owner || '.' || t.table_name ||
  ' IS q''[' || REPLACE(d.comments, '''', '''''') || ']'';' AS ddl
FROM dba_tables t
LEFT JOIN dba_tab_comments tc
  ON tc.owner = t.owner AND tc.table_name = t.table_name
JOIN dict d
  ON UPPER(d.column_name) = UPPER(t.table_name)
WHERE t.owner = :OWNER
  AND (tc.comments IS NULL OR tc.comments = '')
ORDER BY 1;

WITH dict AS (
  SELECT column_name, comments AS standard_comment
  FROM (
    SELECT
        cc.column_name,
        cc.comments,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (
          PARTITION BY cc.column_name
          ORDER BY COUNT(*) DESC, MIN(cc.table_name) ASC
        ) AS rn
    FROM   dba_col_comments cc
    WHERE  cc.owner = :OWNER
      AND  cc.comments IS NOT NULL
    GROUP  BY cc.column_name, cc.comments
  )
  WHERE rn = 1
)
-- 1) 기존 컬럼 주석 DDL
SELECT
  'COMMENT ON COLUMN ' || c.owner || '.' || c.table_name || '.' || c.column_name ||
  ' IS q''[' || REPLACE(cc.comments, '''', '''''') || ']'';' AS ddl
FROM dba_tab_columns c
JOIN dba_col_comments cc
  ON cc.owner = c.owner AND cc.table_name = c.table_name AND cc.column_name = c.column_name
WHERE c.owner = :OWNER
  AND cc.comments IS NOT NULL

UNION ALL

-- 2) 주석이 없는 컬럼 → 표준 주석으로 채워 생성
SELECT
  'COMMENT ON COLUMN ' || c.owner || '.' || c.table_name || '.' || c.column_name ||
  ' IS q''[' || REPLACE(d.standard_comment, '''', '''''') || ']'';' AS ddl
FROM dba_tab_columns c
LEFT JOIN dba_col_comments cc
  ON cc.owner = c.owner AND cc.table_name = c.table_name AND cc.column_name = c.column_name
JOIN dict d
  ON UPPER(d.column_name) = UPPER(c.column_name)
WHERE c.owner = :OWNER
  AND (cc.comments IS NULL OR cc.comments = '')
ORDER BY 1;




