BEGIN
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'STORAGE', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'TABLESPACE', false);
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'CONSTRAINTS', false);      -- CREATE TABLE에 제약 제외
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'REF_CONSTRAINTS', false);  -- CREATE TABLE에 FK 제외
  DBMS_METADATA.set_transform_param(DBMS_METADATA.session_transform, 'SQLTERMINATOR', true);
END;
/

WITH
tgt AS (  -- 대상 테이블만 선필터
  SELECT owner, table_name
  FROM   ALL_TABLES
  WHERE  owner = NVL(:OWNER, USER)
  AND   ( :TABLE_LIKE IS NULL OR table_name LIKE UPPER(:TABLE_LIKE) || '%' )
),
dict AS (  -- 표준 주석 사전: 대상 테이블의 컬럼만 집계(최빈)
  SELECT scc.column_name, scc.comments AS standard_comment
  FROM (
    SELECT cc.column_name, cc.comments,
           COUNT(*) AS cnt,
           ROW_NUMBER() OVER (PARTITION BY cc.column_name
                              ORDER BY COUNT(*) DESC, MIN(cc.table_name) ASC) rn
    FROM   ALL_COL_COMMENTS cc
    JOIN   tgt t ON t.owner=cc.owner AND t.table_name=cc.table_name
    WHERE  cc.comments IS NOT NULL
    GROUP  BY cc.column_name, cc.comments
  ) scc
  WHERE scc.rn = 1
),
tab_cmt AS (  -- 테이블 코멘트: 없으면 표준사전으로 보충
  SELECT t.owner owner_, t.table_name table_name_,
         CASE
           WHEN tc.comments IS NOT NULL AND TRIM(tc.comments) <> '' THEN
             TO_CLOB('COMMENT ON TABLE '||t.owner||'.'||t.table_name||
                     ' IS q''['||REPLACE(tc.comments, '''', '''''')||']'';')
           WHEN d.standard_comment IS NOT NULL THEN
             TO_CLOB('COMMENT ON TABLE '||t.owner||'.'||t.table_name||
                     ' IS q''['||REPLACE(d.standard_comment, '''', '''''')||']'';')
           ELSE TO_CLOB('')
         END ddl_clob
  FROM tgt t
  LEFT JOIN ALL_TAB_COMMENTS tc
    ON tc.owner=t.owner AND tc.table_name=t.table_name
  LEFT JOIN dict d ON UPPER(d.column_name)=UPPER(t.table_name)
),
col_cmt AS (  -- 컬럼 코멘트: 없으면 표준사전으로 보충
  SELECT c.owner owner_, c.table_name table_name_,
         XMLAGG(
           XMLELEMENT(
             "x",
             'COMMENT ON COLUMN '||c.owner||'.'||c.table_name||'.'||c.column_name||
             ' IS q''['||
             REPLACE(COALESCE(NULLIF(TRIM(cc.comments), ''), d.standard_comment), '''', '''''')||
             ']'';'||CHR(10)
           )
           ORDER BY c.column_id
         ).EXTRACT('//text()').getClobVal() ddl_clob
  FROM ALL_TAB_COLUMNS c
  JOIN tgt t ON t.owner=c.owner AND t.table_name=c.table_name
  LEFT JOIN ALL_COL_COMMENTS cc
    ON cc.owner=c.owner AND cc.table_name=c.table_name AND cc.column_name=c.column_name
  LEFT JOIN dict d ON UPPER(d.column_name)=UPPER(c.column_name)
  WHERE (cc.comments IS NOT NULL AND TRIM(cc.comments)<>'' OR d.standard_comment IS NOT NULL)
  GROUP BY c.owner, c.table_name
),
pkuk AS (  -- PK/UK만 생성
  SELECT ac.owner owner_, ac.table_name table_name_,
         XMLAGG(
           XMLELEMENT(
             "x",
             TO_CLOB(
               'ALTER TABLE '||ac.owner||'.'||ac.table_name||
               ' ADD CONSTRAINT '||ac.constraint_name||' '||
               CASE ac.constraint_type WHEN 'P' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END||
               ' ('||
               (SELECT LISTAGG(acc.column_name, ', ') WITHIN GROUP (ORDER BY acc.position)
                  FROM ALL_CONS_COLUMNS acc
                 WHERE acc.owner=ac.owner AND acc.constraint_name=ac.constraint_name)||
               ');'||CHR(10)
             )
           )
           ORDER BY CASE ac.constraint_type WHEN 'P' THEN 1 ELSE 2 END, ac.constraint_name
         ).EXTRACT('//text()').getClobVal() ddl_clob
  FROM ALL_CONSTRAINTS ac
  JOIN tgt t ON t.owner=ac.owner AND t.table_name=ac.table_name
  WHERE ac.constraint_type IN ('P','U')
  GROUP BY ac.owner, ac.table_name
),
fk AS (  -- FK만 생성
  SELECT ac.owner owner_, ac.table_name table_name_,
         XMLAGG(
           XMLELEMENT(
             "x",
             TO_CLOB(
               'ALTER TABLE '||ac.owner||'.'||ac.table_name||
               ' ADD CONSTRAINT '||ac.constraint_name||' FOREIGN KEY ('||
               (SELECT LISTAGG(cc.column_name, ', ') WITHIN GROUP (ORDER BY cc.position)
                  FROM ALL_CONS_COLUMNS cc
                 WHERE cc.owner=ac.owner AND cc.constraint_name=ac.constraint_name)||
               ') REFERENCES '||rcon.owner||'.'||rcon.table_name||' ('||
               (SELECT LISTAGG(rcc.column_name, ', ') WITHIN GROUP (ORDER BY rcc.position)
                  FROM ALL_CONS_COLUMNS rcc
                 WHERE rcc.owner=rcon.owner AND rcc.constraint_name=rcon.constraint_name)||
               ')'||
               CASE ac.delete_rule WHEN 'CASCADE' THEN ' ON DELETE CASCADE' ELSE '' END||
               ';'||CHR(10)
             )
           )
           ORDER BY ac.constraint_name
         ).EXTRACT('//text()').getClobVal() ddl_clob
  FROM ALL_CONSTRAINTS ac
  JOIN ALL_CONSTRAINTS rcon
    ON rcon.owner=ac.r_owner AND rcon.constraint_name=ac.r_constraint_name
  JOIN tgt t ON t.owner=ac.owner AND t.table_name=ac.table_name
  WHERE ac.constraint_type='R'
  GROUP BY ac.owner, ac.table_name
)
SELECT
  t.owner,
  t.table_name,
  DBMS_METADATA.get_ddl('TABLE', t.table_name, t.owner)
  || CASE WHEN pkuk.ddl_clob IS NOT NULL THEN TO_CLOB(CHR(10))||pkuk.ddl_clob ELSE TO_CLOB('') END
  || CASE WHEN fk.ddl_clob   IS NOT NULL THEN TO_CLOB(CHR(10))||fk.ddl_clob   ELSE TO_CLOB('') END
  || CASE WHEN tab.ddl_clob  IS NOT NULL AND LENGTH(tab.ddl_clob)>0
          THEN TO_CLOB(CHR(10))||tab.ddl_clob ELSE TO_CLOB('') END
  || CASE WHEN col.ddl_clob  IS NOT NULL AND LENGTH(col.ddl_clob)>0
          THEN TO_CLOB(CHR(10))||col.ddl_clob ELSE TO_CLOB('') END AS full_ddl
FROM tgt t
LEFT JOIN pkuk     ON pkuk.owner_ = t.owner AND pkuk.table_name_ = t.table_name
LEFT JOIN fk       ON fk.owner_   = t.owner AND fk.table_name_   = t.table_name
LEFT JOIN tab_cmt tab ON tab.owner_ = t.owner AND tab.table_name_ = t.table_name
LEFT JOIN col_cmt col ON col.owner_ = t.owner AND col.table_name_ = t.table_name
ORDER BY t.table_name;
