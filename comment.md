-- 0) DDL 출력 옵션
BEGIN
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'STORAGE', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'TABLESPACE', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'CONSTRAINTS', false);      -- CREATE TABLE엔 제외
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'REF_CONSTRAINTS', false);  -- CREATE TABLE엔 제외
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SQLTERMINATOR', true);     -- ; 붙이기
END;
/

WITH
-- 1) 스키마 내 "컬럼명 → 표준 주석" 사전 (최빈값 채택)
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
    FROM   ALL_COL_COMMENTS cc
    WHERE  cc.owner = NVL(:OWNER, USER)
      AND  cc.comments IS NOT NULL
    GROUP  BY cc.column_name, cc.comments
  )
  WHERE rn = 1
),

-- 2) 테이블 COMMENT (기존 → 없으면 표준사전으로 보충)
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
          TO_CLOB('')
      END AS ddl_clob
  FROM ALL_TABLES t
  LEFT JOIN ALL_TAB_COMMENTS tc
    ON tc.owner = t.owner AND tc.table_name = t.table_name
  LEFT JOIN dict d
    ON UPPER(d.column_name) = UPPER(t.table_name)  -- 테이블명과 같은 컬럼명의 표준주석을 테이블 주석 대체로 사용
  WHERE t.owner = NVL(:OWNER, USER)
),

-- 3) 컬럼 COMMENT (기존 → 없으면 표준사전으로 보충)
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
  FROM ALL_TAB_COLUMNS c
  LEFT JOIN ALL_COL_COMMENTS cc
    ON cc.owner = c.owner AND cc.table_name = c.table_name AND cc.column_name = c.column_name
  LEFT JOIN dict d
    ON UPPER(d.column_name) = UPPER(c.column_name)
  WHERE c.owner = NVL(:OWNER, USER)
    AND (cc.comments IS NOT NULL AND TRIM(cc.comments) <> '' OR d.standard_comment IS NOT NULL)
  GROUP BY c.owner, c.table_name
),

-- 4) 제약조건 DDL (PK/UK/CK/FK)
constraint_lines AS (
  SELECT
      c.owner,
      c.table_name,
      XMLAGG(
        XMLELEMENT(
          "x",
          DBMS_METADATA.get_ddl('CONSTRAINT', c.constraint_name, c.owner) || CHR(10)
        )
        ORDER BY
          CASE c.constraint_type WHEN 'P' THEN 1 WHEN 'U' THEN 2 WHEN 'C' THEN 3 WHEN 'R' THEN 4 ELSE 9 END,
          c.constraint_name
      ).EXTRACT('//text()').getClobVal() AS ddl_clob
  FROM ALL_CONSTRAINTS c
  WHERE c.owner = NVL(:OWNER, USER)
    AND c.constraint_type IN ('P','U','C','R')
    -- AND c.status = 'ENABLED'  -- 활성 제약만 원하면 주석 해제
  GROUP BY c.owner, c.table_name
)

SELECT
    t.owner,
    t.table_name,
    -- 5-1) CREATE TABLE (제약조건 제외)
    DBMS_METADATA.get_ddl('TABLE', t.table_name, t.owner)
    ||
    -- 5-2) 제약조건 (별도 ALTER ADD CONSTRAINT)
    TO_CLOB(CHR(10)) ||
    NVL((SELECT ddl_clob FROM constraint_lines k
         WHERE k.owner = t.owner AND k.table_name = t.table_name), TO_CLOB(''))
    ||
    -- 5-3) 테이블 COMMENT
    TO_CLOB(CHR(10)) ||
    NVL((SELECT ddl_clob FROM table_comment_line x
         WHERE x.owner = t.owner AND x.table_name = t.table_name), TO_CLOB(''))
    ||
    -- 5-4) 컬럼 COMMENT
    TO_CLOB(CHR(10)) ||
    NVL((SELECT ddl_clob FROM column_comment_lines y
         WHERE y.owner = t.owner AND y.table_name = t.table_name), TO_CLOB(''))
    AS full_ddl
FROM ALL_TABLES t
WHERE t.owner = NVL(:OWNER, USER)
  AND ( :TABLE_LIKE IS NULL OR t.table_name LIKE UPPER(:TABLE_LIKE) || '%' )
ORDER BY t.table_name;
