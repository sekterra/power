BEGIN
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'STORAGE', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'TABLESPACE', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'CONSTRAINTS', false);      -- CREATE TABLE에는 제외
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'REF_CONSTRAINTS', false);  -- CREATE TABLE에는 제외
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SQLTERMINATOR', true);
END;
/

WITH
-- 1) 표준 주석 사전
dict AS (
  SELECT scc.column_name AS column_name,
         scc.comments     AS standard_comment
  FROM (
    SELECT cc.column_name,
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
  ) scc
  WHERE scc.rn = 1
),

-- 2) 테이블 코멘트
table_comment_line AS (
  SELECT t.owner        AS owner_,
         t.table_name   AS table_name_,
         CASE
           WHEN tc.comments IS NOT NULL AND TRIM(tc.comments) <> '' THEN
             TO_CLOB(
               'COMMENT ON TABLE ' || t.owner || '.' || t.table_name ||
               ' IS q''[' || REPLACE(tc.comments, '''', '''''') || ']'';'
             )
           WHEN d.standard_comment IS NOT NULL THEN
             TO_CLOB(
               'COMMENT ON TABLE ' || t.owner || '.' || t.table_name ||
               ' IS q''[' || REPLACE(d.standard_comment, '''', '''''') || ']'';'
             )
           ELSE TO_CLOB('')
         END AS ddl_clob
  FROM ALL_TABLES t
  LEFT JOIN ALL_TAB_COMMENTS tc
    ON tc.owner = t.owner
   AND tc.table_name = t.table_name
  LEFT JOIN dict d
    ON UPPER(d.column_name) = UPPER(t.table_name)  -- 테이블명과 같은 컬럼명의 표준주석 사용
  WHERE t.owner = NVL(:OWNER, USER)
),

-- 3) 컬럼 코멘트
column_comment_lines AS (
  SELECT c.owner       AS owner_,
         c.table_name  AS table_name_,
         XMLAGG(
           XMLELEMENT(
             "x",
             'COMMENT ON COLUMN ' || c.owner || '.' || c.table_name || '.' || c.column_name ||
             ' IS q''[' ||
             REPLACE(COALESCE(NULLIF(TRIM(cc.comments), ''), d.standard_comment), '''', '''''') ||
             ']'';' || CHR(10)
           )
           ORDER BY c.column_id
         ).EXTRACT('//text()').getClobVal() AS ddl_clob
  FROM ALL_TAB_COLUMNS c
  LEFT JOIN ALL_COL_COMMENTS cc
    ON cc.owner = c.owner
   AND cc.table_name = c.table_name
   AND cc.column_name = c.column_name
  LEFT JOIN dict d
    ON UPPER(d.column_name) = UPPER(c.column_name)
  WHERE c.owner = NVL(:OWNER, USER)
    AND (cc.comments IS NOT NULL AND TRIM(cc.comments) <> '' OR d.standard_comment IS NOT NULL)
  GROUP BY c.owner, c.table_name
),

-- 4-A) PK/UK
pk_uk_lines AS (
  SELECT ac.owner       AS owner_,
         ac.table_name  AS table_name_,
         XMLAGG(
           XMLELEMENT(
             "x",
             TO_CLOB(
               'ALTER TABLE ' || ac.owner || '.' || ac.table_name ||
               ' ADD CONSTRAINT ' || ac.constraint_name || ' ' ||
               CASE ac.constraint_type WHEN 'P' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END ||
               ' (' ||
               (SELECT LISTAGG(acc.column_name, ', ') WITHIN GROUP (ORDER BY acc.position)
                  FROM ALL_CONS_COLUMNS acc
                 WHERE acc.owner = ac.owner
                   AND acc.constraint_name = ac.constraint_name) ||
               ');' || CHR(10)
             )
           )
           ORDER BY
             CASE ac.constraint_type WHEN 'P' THEN 1 ELSE 2 END,
             ac.constraint_name
         ).EXTRACT('//text()').getClobVal() AS ddl_clob
  FROM ALL_CONSTRAINTS ac
  WHERE ac.owner = NVL(:OWNER, USER)
    AND ac.constraint_type IN ('P','U')
  GROUP BY ac.owner, ac.table_name
),

-- 4-B) FK (참조대상 포함)
fk_lines AS (
  SELECT ac.owner      AS owner_,
         ac.table_name AS table_name_,
         XMLAGG(
           XMLELEMENT(
             "x",
             TO_CLOB(
               'ALTER TABLE ' || ac.owner || '.' || ac.table_name ||
               ' ADD CONSTRAINT ' || ac.constraint_name || ' FOREIGN KEY (' ||
               (SELECT LISTAGG(cc.column_name, ', ') WITHIN GROUP (ORDER BY cc.position)
                  FROM ALL_CONS_COLUMNS cc
                 WHERE cc.owner = ac.owner
                   AND cc.constraint_name = ac.constraint_name) ||
               ') REFERENCES ' || rcon.owner || '.' || rcon.table_name || ' (' ||
               (SELECT LISTAGG(rcc.column_name, ', ') WITHIN GROUP (ORDER BY rcc.position)
                  FROM ALL_CONS_COLUMNS rcc
                 WHERE rcc.owner = rcon.owner
                   AND rcc.constraint_name = rcon.constraint_name) ||
               ')' ||
               CASE ac.delete_rule WHEN 'CASCADE' THEN ' ON DELETE CASCADE' ELSE '' END ||
               ';' || CHR(10)
             )
           )
           ORDER BY ac.constraint_name
         ).EXTRACT('//text()').getClobVal() AS ddl_clob
  FROM ALL_CONSTRAINTS ac
  JOIN ALL_CONSTRAINTS rcon
    ON rcon.owner = ac.r_owner
   AND rcon.constraint_name = ac.r_constraint_name
  WHERE ac.owner = NVL(:OWNER, USER)
    AND ac.constraint_type = 'R'
  GROUP BY ac.owner, ac.table_name
),

-- 4-C) CK (11g: LONG → DBMS_XMLGEN으로 CLOB 추출)
ck_lines AS (
  SELECT ac.owner      AS owner_,
         ac.table_name AS table_name_,
         XMLAGG(
           XMLELEMENT(
             "x",
             TO_CLOB(
               'ALTER TABLE ' || ac.owner || '.' || ac.table_name ||
               ' ADD CONSTRAINT ' || ac.constraint_name ||
               ' CHECK (' ||
               -- LONG search_condition → CLOB
               (
                 SELECT XMLTYPE(
                          DBMS_XMLGEN.getXML(
                            'SELECT search_condition FROM all_constraints ' ||
                            'WHERE owner = ''' || ac.owner || ''' ' ||
                            'AND constraint_name = ''' || ac.constraint_name || ''''
                          )
                        ).EXTRACT('//SEARCH_CONDITION/text()').getClobVal()
                 FROM dual
               ) ||
               ');' || CHR(10)
             )
           )
           ORDER BY ac.constraint_name
         ).EXTRACT('//text()').getClobVal() AS ddl_clob
  FROM ALL_CONSTRAINTS ac
  WHERE ac.owner = NVL(:OWNER, USER)
    AND ac.constraint_type = 'C'
  GROUP BY ac.owner, ac.table_name
),

-- 4-D) 제약 통합 (테이블별)
constraint_lines AS (
  SELECT base.owner_ AS owner_,
         base.table_name_ AS table_name_,
         NVL(pk.ddl_clob, TO_CLOB('')) ||
         NVL(ck.ddl_clob, TO_CLOB('')) ||
         NVL(fk.ddl_clob, TO_CLOB('')) AS ddl_clob
  FROM (
    SELECT DISTINCT t.owner_ , t.table_name_
    FROM (
      SELECT p.owner_ , p.table_name_ FROM pk_uk_lines p
      UNION ALL
      SELECT c.owner_ , c.table_name_ FROM ck_lines c
      UNION ALL
      SELECT f.owner_ , f.table_name_ FROM fk_lines f
    ) t
  ) base
  LEFT JOIN pk_uk_lines pk ON pk.owner_ = base.owner_ AND pk.table_name_ = base.table_name_
  LEFT JOIN ck_lines    ck ON ck.owner_ = base.owner_ AND ck.table_name_ = base.table_name_
  LEFT JOIN fk_lines    fk ON fk.owner_ = base.owner_ AND fk.table_name_ = base.table_name_
)

SELECT
  t.owner        AS owner,
  t.table_name   AS table_name,
  -- CREATE TABLE (제약 제외)
  DBMS_METADATA.get_ddl('TABLE', t.table_name, t.owner)
  ||
  -- 제약 (없으면 SKIP)
  CASE
    WHEN EXISTS (
      SELECT 1 FROM constraint_lines k
      WHERE k.owner_ = t.owner
        AND k.table_name_ = t.table_name
        AND k.ddl_clob IS NOT NULL
        AND LENGTH(k.ddl_clob) > 0
    )
    THEN TO_CLOB(CHR(10)) ||
         (SELECT k.ddl_clob
            FROM constraint_lines k
           WHERE k.owner_ = t.owner
             AND k.table_name_ = t.table_name)
    ELSE TO_CLOB('')
  END
  ||
  -- 테이블 COMMENT
  CASE
    WHEN EXISTS (
      SELECT 1 FROM table_comment_line x
      WHERE x.owner_ = t.owner
        AND x.table_name_ = t.table_name
        AND x.ddl_clob IS NOT NULL
        AND LENGTH(x.ddl_clob) > 0
    )
    THEN TO_CLOB(CHR(10)) ||
         (SELECT x.ddl_clob
            FROM table_comment_line x
           WHERE x.owner_ = t.owner
             AND x.table_name_ = t.table_name)
    ELSE TO_CLOB('')
  END
  ||
  -- 컬럼 COMMENT
  CASE
    WHEN EXISTS (
      SELECT 1 FROM column_comment_lines y
      WHERE y.owner_ = t.owner
        AND y.table_name_ = t.table_name
        AND y.ddl_clob IS NOT NULL
        AND LENGTH(y.ddl_clob) > 0
    )
    THEN TO_CLOB(CHR(10)) ||
         (SELECT y.ddl_clob
            FROM column_comment_lines y
           WHERE y.owner_ = t.owner
             AND y.table_name_ = t.table_name)
    ELSE TO_CLOB('')
  END AS full_ddl
FROM ALL_TABLES t
WHERE t.owner = NVL(:OWNER, USER)
  AND ( :TABLE_LIKE IS NULL OR t.table_name LIKE UPPER(:TABLE_LIKE) || '%' )
ORDER BY t.table_name;
