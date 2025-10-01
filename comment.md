-- 0) DDL 출력 옵션(원하는 대로 조정하세요)
BEGIN
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'STORAGE', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'TABLESPACE', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'CONSTRAINTS', true);     -- PK/UK/CK 포함
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'REF_CONSTRAINTS', true); -- FK 포함
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SQLTERMINATOR', true);   -- ; 붙이기
END;
/

WITH
-- 1) 컬럼명 기준 "표준 주석 사전": 한 스키마 내 가장 많이 쓰인 주석을 표준으로 채택
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

-- 2) 테이블 코멘트 DDL (기존 → 없으면 표준사전으로 보충)
table_comment_line AS (
  SELECT
      t.owner,
      t.table_name,
      CASE
        WHEN tc.comments IS NOT NULL AND TRIM(tc.comments) <> '' THEN
          'COMMENT ON TABLE ' || t.owner || '.' || t.table_name ||
          ' IS q''[' || REPLACE(tc.comments, '''', '''''') || ']'';'
        WHEN d.standard_comment IS NOT NULL THEN
          'COMMENT ON TABLE ' || t.owner || '.' || t.table_name ||
          ' IS q''[' || REPLACE(d.standard_comment, '''', '''''') || ']'';'
        ELSE
          NULL
      END AS ddl
  FROM dba_tables t
  LEFT JOIN dba_tab_comments tc
    ON tc.owner = t.owner AND tc.table_name = t.table_name
  LEFT JOIN dict d
    ON UPPER(d.column_name) = UPPER(t.table_name)  -- 테이블명과 동일한 컬럼명이 있을 때 그 표준주석 사용
  WHERE t.owner = :OWNER
),

-- 3) 컬럼 코멘트 DDL (기존 → 없으면 표준사전으로 보충)
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
    -- 4-1) CREATE TABLE (제약조건 포함)
    DBMS_METADATA.get_ddl('TABLE', t.table_name, t.owner)
    ||
    -- 4-2) (선택) 인덱스 DDL: 필요 없으면 이 블록 삭제
    NVL(DBMS_METADATA.get_dependent_ddl('INDEX', t.table_name, t.owner), '')
    ||
    -- 4-3) 테이블 코멘트 (있으면/보충)
    CASE
      WHEN EXISTS (SELECT 1 FROM table_comment_line x
                   WHERE x.owner = t.owner AND x.table_name = t.table_name AND x.ddl IS NOT NULL)
      THEN CHR(10) || (SELECT ddl FROM table_comment_line x
                        WHERE x.owner = t.owner AND x.table_name = t.table_name)
      ELSE ''
    END
    ||
    -- 4-4) 컬럼 코멘트들 (있으면/보충)
    CASE
      WHEN EXISTS (SELECT 1 FROM column_comment_lines y
                   WHERE y.owner = t.owner AND y.table_name = t.table_name)
      THEN CHR(10) || (SELECT ddl_clob FROM column_comment_lines y
                       WHERE y.owner = t.owner AND y.table_name = t.table_name)
      ELSE ''
    END
    AS full_ddl
FROM dba_tables t
WHERE t.owner = :OWNER
  AND (:TABLE_LIKE IS NULL OR t.table_name LIKE UPPER(:TABLE_LIKE))
ORDER BY t.table_name;
