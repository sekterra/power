BEGIN
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'STORAGE', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'TABLESPACE', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'CONSTRAINTS', true);     
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'REF_CONSTRAINTS', true);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SQLTERMINATOR', true);
END;
/

WITH
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

-- CLOB로 만들어 둡니다
table_comment_line AS (
  SELECT
      t.owner,
      t.table_name,
      CASE
        WHEN tc.comments IS NOT NULL AND TRIM(tc.comments) <> '' THEN
          TO_CLOB('COMMENT ON TABLE ' || t.owner || '.' || t.table_name ||
                  ' IS q''[' || REPLACE(tc.comments, '''', '''''') || ']'';')
        WHEN d.standard_comment IS NOT NULL THEN
          TO_CLOB('COMMENT ON TABLE ' || t.owner || '.' || t.table_name ||
                  ' IS q''[' || REPLACE(d.standard_comment, '''', '''''') || ']'';')
        ELSE
          TO_CLOB(NULL)
      END AS ddl_clob
  FROM dba_tables t
  LEFT JOIN dba_tab_comments tc
    ON tc.owner = t.owner AND tc.table_name = t.table_name
  LEFT JOIN dict d
    ON UPPER(d.column_name) = UPPER(t.table_name)
  WHERE t.owner = :OWNER
),

-- 이미 CLOB를 생성하도록 구성
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
            COALESCE(NULLIF(TRIM(cc.comments), ''), d.standard_comment),
            '''', ''''''
          ) || ']'';' || CHR(10)
        )
        ORDER BY c.column_id
      ).EXTRACT('//text()').getClobVal() AS ddl_clob
  FROM dba_tab_columns c
  LEFT JOIN dba_col_comments cc
    ON cc.owner = c.owner AND cc.table_name = c.table_name AND cc.column_name = c.column_name
  LEFT JOIN dict d
    ON UPPER(d.column_name) = UPPER(c.column_name)
  WHERE c.owner = :OWNER
    AND (cc.comments IS NOT NULL AND TRIM(cc.comments) <> '' OR d.standard_comment IS NOT NULL)
  GROUP BY c.owner, c.table_name
)

SELECT
    t.owner,
    t.table_name,
    -- CREATE TABLE (CLOB)
    DBMS_METADATA.get_ddl('TABLE', t.table_name, t.owner)
    ||
    -- 인덱스 DDL (없으면 빈 CLOB)
    NVL(DBMS_METADATA.get_dependent_ddl('INDEX', t.table_name, t.owner), TO_CLOB(''))
    ||
    -- 개행
    TO_CLOB(CHR(10))
    ||
    -- 테이블 코멘트 (없으면 빈 CLOB)
    NVL(
      (SELECT ddl_clob FROM table_comment_line x
        WHERE x.owner = t.owner AND x.table_name = t.table_name),
      TO_CLOB('')
    )
    ||
    -- 개행
    TO_CLOB(CHR(10))
    ||
    -- 컬럼 코멘트들 (없으면 빈 CLOB)
    NVL(
      (SELECT ddl_clob FROM column_comment_lines y
        WHERE y.owner = t.owner AND y.table_name = t.table_name),
      TO_CLOB('')
    ) AS full_ddl
FROM dba_tables t
WHERE t.owner = :OWNER
  AND (:TABLE_LIKE IS NULL OR t.table_name LIKE UPPER(:TABLE_LIKE))
ORDER BY t.table_name;
