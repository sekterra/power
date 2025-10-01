-- 0) DDL 옵션: TABLE은 제약조건/참조제약 제외해서 뽑고, SQL 끝에 ; 붙이기
BEGIN
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'STORAGE', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'TABLESPACE', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'CONSTRAINTS', false);      -- 테이블 DDL에서는 제외
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'REF_CONSTRAINTS', false);  -- 테이블 DDL에서는 제외
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SQLTERMINATOR', true);     -- ; 붙이기
END;
/

WITH
-- 1) 스키마 내 "컬럼명 → 표준 주석" 사전 (가장 많이 쓰인 주석 채택)
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

-- 2) 테이블 COMMENT 라인 (기존 → 없으면 사전으로 보충)
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
  FROM dba_tables t
  LEFT JOIN dba_tab_comments tc
    ON tc.owner = t.owner AND tc.table_name = t.table_name
  LEFT JOIN dict d
    ON UPPER(d.column_name) = UPPER(t.table_name)  -- 테이블명=컬럼명일 때 표준 주석을 테이블 주석 대체
  WHERE t.owner = :OWNER
),

-- 3) 컬럼 COMMENT 라인들 (기존 → 없으면 사전으로 보충)
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
),

-- 4) 제약조건 DDL (P/U/C/R 모두, DBA_CONSTRAINTS를 기준으로 선별하여 GET_DDL로 생성/집계)
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
          CASE c.constraint_type  -- 보기 좋게 PK→UK→CK→FK 순서
            WHEN 'P' THEN 1 WHEN 'U' THEN 2 WHEN 'C' THEN 3 WHEN 'R' THEN 4 ELSE 9 END,
          c.constraint_name
      ).EXTRACT('//text()').getClobVal() AS ddl_clob
  FROM dba_constraints c
  WHERE c.owner = :OWNER
    AND c.constraint_type IN ('P','U','C','R')  -- PK/Unique/Check/Foreign
    -- 상태/유효성 조건: 필요 시 아래 라인 활성화
    -- AND c.status = 'ENABLED'
  GROUP BY c.owner, c.table_name
)

SELECT
    t.owner,
    t.table_name,
    -- 5-1) CREATE TABLE (제약조건 없이)
    DBMS_METADATA.get_ddl('TABLE', t.table_name, t.owner)
    ||
    -- 5-2) 제약조건 DDL (DBA_CONSTRAINTS 기반)
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
FROM dba_tables t
WHERE t.owner = :OWNER
  AND (:TABLE_LIKE IS NULL OR t.table_name LIKE UPPER(:TABLE_LIKE))
ORDER BY t.table_name;
