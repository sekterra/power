-- 0) DDL 출력 옵션 (CREATE TABLE에서 제약 제외, SQL 끝에 ;)
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
-- 1) 컬럼명 → 표준 주석(최빈) 사전
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

-- 2) 테이블 COMMENT (원본 → 없으면 표준사전 대체)
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
    ON UPPER(d.column_name) = UPPER(t.table_name)
  WHERE t.owner = NVL(:OWNER, USER)
),

-- 3) 컬럼 COMMENT (원본 → 없으면 표준사전 대체)
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

-- 4-A) PK/UK DDL 생성
pk_uk_lines AS (
  SELECT
      ac.owner,
      ac.table_name,
      XMLAGG(
        XMLELEMENT(
          "x",
          TO_CLOB('ALTER TABLE ' || ac.owner || '.' || ac.table_name ||
                  ' ADD CONSTRAINT ' || ac.constraint_name || ' ' ||
                  CASE ac.constraint_type WHEN 'P' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END ||
                  ' (' ||
                  (SELECT LISTAGG(acc.column_name, ', ') WITHIN GROUP (ORDER BY acc.position)
                     FROM ALL_CONS_COLUMNS acc
                    WHERE acc.owner = ac.owner AND acc.constraint_name = ac.constraint_name)
                  || ');' || CHR(10))
        )
        ORDER BY CASE ac.constraint_type WHEN 'P' THEN 1 ELSE 2 END, ac.constraint_name
      ).EXTRACT('//text()').getClobVal() AS ddl_clob
  FROM ALL_CONSTRAINTS ac
  WHERE ac.owner = NVL(:OWNER, USER)
    AND ac.constraint_type IN ('P','U')
  GROUP BY ac.owner, ac.table_name
),

-- 4-B) FK DDL 생성 (ON DELETE 규칙 반영)
fk_lines AS (
  SELECT
      ac.owner,
      ac.table_name,
      XMLAGG(
        XMLELEMENT(
          "x",
          TO_CLOB('ALTER TABLE ' || ac.owner || '.' || ac.table_name ||
                  ' ADD CONSTRAINT ' || ac.constraint_name || ' FOREIGN KEY (' ||
                  (SELECT LISTAGG(cc.column_name, ', ') WITHIN GROUP (ORDER BY cc.position)
                     FROM ALL_CONS_COLUMNS cc
                    WHERE cc.owner = ac.owner AND cc.constraint_name = ac.constraint_name) ||
                  ') REFERENCES ' || rcon.owner || '.' || rcon.table_name || ' (' ||
                  (SELECT LISTAGG(rcc.column_name, ', ') WITHIN GROUP (ORDER BY rcc.position)
                     FROM ALL_CONS_COLUMNS rcc
                    WHERE rcc.owner = rcon.owner AND rcc.constraint_name = rcon.constraint_name) ||
                  ')' ||
                  CASE ac.delete_rule WHEN 'CASCADE' THEN ' ON DELETE CASCADE' ELSE '' END ||
                  ';' || CHR(10))
        )
        ORDER BY ac.constraint_name
      ).EXTRACT('//text()').getClobVal() AS ddl_clob
  FROM ALL_CONSTRAINTS ac
  JOIN ALL_CONSTRAINTS rcon
    ON rcon.owner = ac.r_owner AND rcon.constraint_name = ac.r_constraint_name
  WHERE ac.owner = NVL(:OWNER, USER)
    AND ac.constraint_type = 'R'
  GROUP BY ac.owner, ac.table_name
),

-- 4-C) CHECK 제약 DDL 생성 (SEARCH_CONDITION_VC 있을 때만)
ck_lines AS (
  SELECT
      ac.owner,
      ac.table_name,
      XMLAGG(
        XMLELEMENT(
          "x",
          TO_CLOB('ALTER TABLE ' || ac.owner || '.' || ac.table_name ||
                  ' ADD CONSTRAINT ' || ac.constraint_name ||
                  ' CHECK (' || ac.search_condition_vc || ');' || CHR(10))
        )
        ORDER BY ac.constraint_name
      ).EXTRACT('//text()').getClobVal() AS ddl_clob
  FROM ALL_CONSTRAINTS ac
  WHERE ac.owner = NVL(:OWNER, USER)
    AND ac.constraint_type = 'C'
    AND ac.search_condition_vc IS NOT NULL     -- 구버전(컬럼 없으면) 자동 SKIP
  GROUP BY ac.owner, ac.table_name
),

-- 4-D) 제약 DDL 통합 (테이블별)
constraint_lines AS (
  SELECT owner, table_name,
         -- PK/UK → CK → FK 순으로 보기 좋게 이어붙임
         NVL(pk.ddl_clob, TO_CLOB('')) ||
         NVL(ck.ddl_clob, TO_CLOB('')) ||
         NVL(fk.ddl_clob, TO_CLOB('')) AS ddl_clob
  FROM (
    SELECT DISTINCT owner, table_name
    FROM (
      SELECT owner, table_name FROM pk_uk_lines
      UNION SELECT owner, table_name FROM ck_lines
      UNION SELECT owner, table_name FROM fk_lines
    )
  ) t
  LEFT JOIN pk_uk_lines pk ON pk.owner = t.owner AND pk.table_name = t.table_name
  LEFT JOIN ck_lines    ck ON ck.owner = t.owner AND ck.table_name = t.table_name
  LEFT JOIN fk_lines    fk ON fk.owner = t.owner AND fk.table_name = t.table_name
)

SELECT
    t.owner,
    t.table_name,
    -- 5-1) CREATE TABLE (제약 제외)
    DBMS_METADATA.get_ddl('TABLE', t.table_name, t.owner)
    ||
    -- 5-2) 제약 DDL (해당 테이블에 제약이 없으면 자동 SKIP)
    CASE
      WHEN EXISTS (SELECT 1 FROM constraint_lines k
                   WHERE k.owner = t.owner AND k.table_name = t.table_name
                     AND k.ddl_clob IS NOT NULL AND LENGTH(k.ddl_clob) > 0)
      THEN TO_CLOB(CHR(10)) ||
           (SELECT k.ddl_clob FROM constraint_lines k
             WHERE k.owner = t.owner AND k.table_name = t.table_name)
      ELSE TO_CLOB('')
    END
    ||
    -- 5-3) 테이블 COMMENT
    CASE
      WHEN EXISTS (SELECT 1 FROM table_comment_line x
                   WHERE x.owner = t.owner AND x.table_name = t.table_name
                     AND x.ddl_clob IS NOT NULL AND LENGTH(x.ddl_clob) > 0)
      THEN TO_CLOB(CHR(10)) ||
           (SELECT x.ddl_clob FROM table_comment_line x
             WHERE x.owner = t.owner AND x.table_name = t.table_name)
      ELSE TO_CLOB('')
    END
    ||
    -- 5-4) 컬럼 COMMENT
    CASE
      WHEN EXISTS (SELECT 1 FROM column_comment_lines y
                   WHERE y.owner = t.owner AND y.table_name = t.table_name
                     AND y.ddl_clob IS NOT NULL AND LENGTH(y.ddl_clob) > 0)
      THEN TO_CLOB(CHR(10)) ||
           (SELECT y.ddl_clob FROM column_comment_lines y
             WHERE y.owner = t.owner AND y.table_name = t.table_name)
      ELSE TO_CLOB('')
    END
    AS full_ddl
FROM ALL_TABLES t
WHERE t.owner = NVL(:OWNER, USER)
  AND ( :TABLE_LIKE IS NULL OR t.table_name LIKE UPPER(:TABLE_LIKE) || '%' )
ORDER BY t.table_name;
