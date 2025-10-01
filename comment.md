- 테이블별 CREATE TABLE + COMMENT ON ... 을 하나의 CLOB으로 생성
WITH
-- 1) 컬럼명 기반 "표준 주석 사전" (스키마 내 가장 많이 쓰인 주석)
dict AS (
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
),

-- 2) 테이블 코멘트(실존/대체) 라인
table_comment_line AS (
  SELECT
      t.owner,
      t.table_name,
      CASE
        WHEN tc.comments IS NOT NULL AND TRIM(tc.comments) <> '' THEN
          'COMMENT ON TABLE ' || t.owner || '.' || t.table_name ||
          ' IS q''[' || REPLACE(tc.comments, '''', '''''') || ']'';'
        WHEN d.standard_comment IS NOT NULL THEN
          -- 테이블 코멘트가 없을 때: 컬럼명=테이블명인 표준 주석을 대체 사용
          'COMMENT ON TABLE ' || t.owner || '.' || t.table_name ||
          ' IS q''[' || REPLACE(d.standard_comment, '''', '''''') || ']'';'
        ELSE
          NULL
      END AS ddl
  FROM dba_tables t
  LEFT JOIN dba_tab_comments tc
    ON tc.owner = t.owner AND tc.table_name = t.table_name
  LEFT JOIN dict d
    ON UPPER(d.column_name) = UPPER(t.table_name)
  WHERE t.owner = :OWNER
),

-- 3) 컬럼 코멘트(실존/대체) 라인들
column_comment_lines AS (
  SELECT
      c.owner,
      c.table_name,
      XMLAGG(
        XMLELEMENT(
          "x",
          'COMMENT ON COLUMN ' || c.owner || '.' || c.table_name || '.' || c.column_name ||
          ' IS q''[' ||
          REPLACE(
            -- 실존 코멘트 우선, 없으면 표준 사전으로 대체
            COALESCE(NULLIF(TRIM(cc.comments), ''), d.standard_comment),
            '''', ''''''
          ) || ']'';'
        )
        ORDER BY c.column_id
      ).EXTRACT('//text()').getClobVal() AS ddl_clob
  FROM dba_tab_columns c
  LEFT JOIN dba_col_comments cc
    ON cc.owner = c.owner AND cc.table_name = c.table_name AND cc.column_name = c.column_name
  LEFT JOIN dict d
    ON UPPER(d.column_name) = UPPER(c.column_name)
  WHERE c.owner = :OWNER
    -- 실존 코멘트 또는 표준 사전이 있는 컬럼만 출력
    AND (cc.comments IS NOT NULL AND TRIM(cc.comments) <> '' OR d.standard_comment IS NOT NULL)
  GROUP BY c.owner, c.table_name
)

-- 4) 최종: CREATE TABLE + (테이블 코멘트) + (컬럼 코멘트들)
SELECT
    t.owner,
    t.table_name,
    DBMS_METADATA.get_ddl('TABLE', t.table_name, t.owner)
      || CHR(10)
      || NVL((SELECT ddl FROM table_comment_line x
              WHERE x.owner = t.owner AND x.table_name = t.table_name), '')
      || CASE
           WHEN EXISTS (
              SELECT 1 FROM column_comment_lines y
              WHERE y.owner = t.owner AND y.table_name = t.table_name
           )
           THEN CHR(10) || (SELECT ddl_clob FROM column_comment_lines y
                            WHERE y.owner = t.owner AND y.table_name = t.table_name)
           ELSE ''
         END AS full_ddl
FROM dba_tables t
WHERE t.owner = :OWNER
ORDER BY t.table_name;
