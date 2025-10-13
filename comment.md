BEGIN
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);
END;
/

WITH
params AS (
  SELECT
    UPPER(NVL('&&OWNER_LIKE','%')) AS owner_like,
    UPPER(NVL('&&TABLE_LIKE','%')) AS table_like
  FROM dual
),
targets AS (
  SELECT t.owner, t.table_name
  FROM all_tables t
  CROSS JOIN params p
  WHERE UPPER(t.owner) LIKE p.owner_like
    AND UPPER(t.table_name) LIKE p.table_like
),
create_ddl AS (
  SELECT t.owner, t.table_name, 0 AS seq, 0 AS col_order,
         TO_CLOB('-- ===== 테이블 구분선 =====' || CHR(10)) ||
         DBMS_METADATA.GET_DDL('TABLE', t.table_name, t.owner) AS line_clob
  FROM targets t
),
orig_tab_cmt AS (
  SELECT atc.owner, atc.table_name, atc.comments
  FROM all_tab_comments atc
  JOIN targets t ON t.owner=atc.owner AND t.table_name=atc.table_name
),
orig_col_cmt AS (
  SELECT acc.owner, acc.table_name, acc.column_name, acc.comments
  FROM all_col_comments acc
  JOIN targets t ON t.owner=acc.owner AND t.table_name=acc.table_name
),
tab_dict_base AS (
  SELECT
    REGEXP_REPLACE(
      REGEXP_REPLACE(UPPER(atc.table_name), '^(TBL_|TB_|T_|CM_|IF_|DM_|CD_|DT_|VW_|MV_|X_)+', ''),
      '[0-9]', ''
    ) AS norm_name,
    atc.comments
  FROM all_tab_comments atc
  WHERE atc.comments IS NOT NULL
),
tab_dict_rank AS (
  SELECT norm_name, comments, COUNT(*) AS freq,
         ROW_NUMBER() OVER (PARTITION BY norm_name ORDER BY COUNT(*) DESC, LENGTH(comments) DESC) AS rn
  FROM tab_dict_base
  GROUP BY norm_name, comments
),
tab_dict AS (
  SELECT norm_name, comments AS dict_comment
  FROM tab_dict_rank
  WHERE rn=1
),
col_dict_rank AS (
  SELECT UPPER(acc.column_name) AS col_name, acc.comments, COUNT(*) AS freq,
         ROW_NUMBER() OVER (PARTITION BY UPPER(acc.column_name) ORDER BY COUNT(*) DESC, LENGTH(acc.comments) DESC) AS rn
  FROM all_col_comments acc
  WHERE acc.comments IS NOT NULL
  GROUP BY UPPER(acc.column_name), acc.comments
),
col_dict AS (
  SELECT col_name, comments AS dict_comment
  FROM col_dict_rank
  WHERE rn=1
),
final_tab_cmt AS (
  SELECT t.owner, t.table_name,
         CASE
           WHEN otc.comments IS NOT NULL THEN 'ORIG:'||otc.comments
           ELSE CASE WHEN td.dict_comment IS NOT NULL THEN 'DICT:'||td.dict_comment END
         END AS tagged_comment
  FROM targets t
  LEFT JOIN orig_tab_cmt otc
    ON otc.owner=t.owner AND otc.table_name=t.table_name
  LEFT JOIN (
    SELECT tt.owner, tt.table_name, td.dict_comment
    FROM targets tt
    LEFT JOIN tab_dict td
      ON td.norm_name = REGEXP_REPLACE(
           REGEXP_REPLACE(UPPER(tt.table_name), '^(TBL_|TB_|T_|CM_|IF_|DM_|CD_|DT_|VW_|MV_|X_)+', ''),
           '[0-9]', ''
         )
  ) td ON td.owner=t.owner AND td.table_name=t.table_name
),
final_col_cmt AS (
  SELECT c.owner, c.table_name, c.column_name, c.column_id,
         CASE
           WHEN occ.comments IS NOT NULL THEN 'ORIG:'||occ.comments
           ELSE CASE WHEN cd.dict_comment IS NOT NULL THEN 'DICT:'||cd.dict_comment END
         END AS tagged_comment
  FROM all_tab_columns c
  JOIN targets t ON t.owner=c.owner AND t.table_name=c.table_name
  LEFT JOIN orig_col_cmt occ
    ON occ.owner=c.owner AND occ.table_name=c.table_name AND occ.column_name=c.column_name
  LEFT JOIN col_dict cd
    ON cd.col_name = UPPER(c.column_name)
),
tab_comment_line AS (
  SELECT
    ftc.owner, ftc.table_name, 1 AS seq, 0 AS col_order,
    TO_CLOB(
      'COMMENT ON TABLE "'||ftc.owner||'"."'||ftc.table_name||'" IS ' ||
      CASE
        WHEN ftc.tagged_comment IS NULL THEN 'NULL'
        ELSE
          ''''|| REPLACE(
                  SUBSTR(
                    CASE
                      WHEN SUBSTR(ftc.tagged_comment,1,5)='ORIG:' THEN SUBSTR(ftc.tagged_comment,6)
                      WHEN SUBSTR(ftc.tagged_comment,1,5)='DICT:' THEN '[사전] '||SUBSTR(ftc.tagged_comment,6)
                      ELSE ftc.tagged_comment
                    END, 1, 3900
                  ), '''', ''''''
                ) || ''''
      END ||
      CASE
        WHEN ftc.tagged_comment IS NULL THEN ''
        WHEN SUBSTR(ftc.tagged_comment,1,5)='ORIG:' THEN ' ; -- 원본'
        ELSE ' ; -- 자동(사전)'
      END
    ) AS line_clob
  FROM final_tab_cmt ftc
),
col_comment_line AS (
  SELECT
    fcc.owner, fcc.table_name, 2 AS seq, fcc.column_id AS col_order,
    TO_CLOB(
      'COMMENT ON COLUMN "'||fcc.owner||'"."'||fcc.table_name||'"."'||fcc.column_name||'" IS ' ||
      CASE
        WHEN fcc.tagged_comment IS NULL THEN 'NULL'
        ELSE
          ''''|| REPLACE(
                  SUBSTR(
                    CASE
                      WHEN SUBSTR(fcc.tagged_comment,1,5)='ORIG:' THEN SUBSTR(fcc.tagged_comment,6)
                      WHEN SUBSTR(fcc.tagged_comment,1,5)='DICT:' THEN '[사전] '||SUBSTR(fcc.tagged_comment,6)
                      ELSE fcc.tagged_comment
                    END, 1, 3900
                  ), '''', ''''''
                ) || ''''
      END ||
      CASE
        WHEN fcc.tagged_comment IS NULL THEN ''
        WHEN SUBSTR(fcc.tagged_comment,1,5)='ORIG:' THEN ' ; -- 원본'
        ELSE ' ; -- 자동(사전)'
      END
    ) AS line_clob
  FROM final_col_cmt fcc
),
all_lines AS (
  SELECT owner, table_name, seq, col_order,
         CASE
           WHEN seq = 0 AND REGEXP_LIKE(line_clob, ';\s*$', 'n') THEN line_clob || CHR(10)
           WHEN seq = 0 THEN line_clob || CHR(10) || ';' || CHR(10)
           ELSE line_clob || CHR(10)
         END AS line_with_nl
  FROM (
    SELECT * FROM create_ddl
    UNION ALL
    SELECT * FROM tab_comment_line
    UNION ALL
    SELECT * FROM col_comment_line
  )
)

SELECT
  owner,
  table_name,
  -- XMLSERIALIZE가 만든 엔티티(&quot; 등)를 원문으로 되돌림(플래그 0 = decode)
  DBMS_XMLGEN.CONVERT(
    XMLSERIALIZE(
      CONTENT XMLAGG(
        XMLELEMENT("L", line_with_nl)
        ORDER BY seq, col_order
      ) AS CLOB
    ),
    0
  ) AS create_and_comment_ddl
FROM all_lines
GROUP BY owner, table_name
ORDER BY owner, table_name;
