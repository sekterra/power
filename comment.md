-- =========================================================
-- CREATE TABLE (DBMS_METADATA.GET_DDL) + COMMENT(원본/사전) 스풀
-- - 관리 DDL 테이블 없이 동작
-- - 자신이 접근 가능한 객체 범위에서만 생성
-- - ERD 도구 참조 용도
-- =========================================================

-- (선택) 불필요한 물리 옵션 제거: STORAGE/SEGMENT/TABLESPACE
BEGIN
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);
  -- 제약조건은 ERD 용도에 유용하므로 기본값(TRUE) 유지. 필요시:
  -- DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', TRUE);
END;
/

WITH
params AS (
  SELECT
    UPPER(NVL('&&OWNER_LIKE','%')) AS owner_like,
    UPPER(NVL('&&TABLE_LIKE','%')) AS table_like
  FROM dual
),

-- 1) 대상 테이블 (접근 가능한 테이블만)
targets AS (
  SELECT t.owner, t.table_name
  FROM all_tables t
  CROSS JOIN params p
  WHERE UPPER(t.owner) LIKE p.owner_like
    AND UPPER(t.table_name) LIKE p.table_like
),

-- 2) CREATE TABLE DDL (DBMS_METADATA.GET_DDL)
create_ddl AS (
  SELECT
    t.owner,
    t.table_name,
    1 AS seq,
    DBMS_METADATA.GET_DDL('TABLE', t.table_name, t.owner) AS out_ddl
  FROM targets t
),

-- 3) 원본 코멘트
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

-- 4) 용어 사전(테이블): 정규화명별 최빈 코멘트
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
  SELECT
    norm_name, comments,
    COUNT(*) AS freq,
    ROW_NUMBER() OVER (PARTITION BY norm_name ORDER BY COUNT(*) DESC, LENGTH(comments) DESC) AS rn
  FROM tab_dict_base
  GROUP BY norm_name, comments
),
tab_dict AS (
  SELECT norm_name, comments AS dict_comment
  FROM tab_dict_rank
  WHERE rn=1
),

-- 5) 용어 사전(컬럼): 컬럼명별 최빈 코멘트
col_dict_rank AS (
  SELECT
    UPPER(acc.column_name) AS col_name,
    acc.comments,
    COUNT(*) AS freq,
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

-- 6) 테이블 코멘트(원본 우선, 없으면 사전)
final_tab_cmt AS (
  SELECT
    t.owner, t.table_name,
    CASE
      WHEN otc.comments IS NOT NULL THEN 'ORIG:'||otc.comments
      ELSE CASE
             WHEN td.dict_comment IS NOT NULL THEN 'DICT:'||td.dict_comment
           END
    END AS tagged_comment
  FROM targets t
  LEFT JOIN orig_tab_cmt otc
    ON otc.owner=t.owner AND otc.table_name=t.table_name
  LEFT JOIN (
    SELECT
      tt.owner, tt.table_name,
      td.dict_comment
    FROM targets tt
    LEFT JOIN tab_dict td
      ON td.norm_name = REGEXP_REPLACE(
           REGEXP_REPLACE(UPPER(tt.table_name), '^(TBL_|TB_|T_|CM_|IF_|DM_|CD_|DT_|VW_|MV_|X_)+', ''),
           '[0-9]', ''
         )
  ) td
    ON td.owner=t.owner AND td.table_name=t.table_name
),

-- 7) 컬럼 코멘트(원본 우선, 없으면 사전)
final_col_cmt AS (
  SELECT
    c.owner, c.table_name, c.column_name, c.column_id,
    CASE
      WHEN occ.comments IS NOT NULL THEN 'ORIG:'||occ.comments
      ELSE CASE
             WHEN cd.dict_comment IS NOT NULL THEN 'DICT:'||cd.dict_comment
           END
    END AS tagged_comment
  FROM all_tab_columns c
  JOIN targets t
    ON t.owner=c.owner AND t.table_name=c.table_name
  LEFT JOIN orig_col_cmt occ
    ON occ.owner=c.owner AND occ.table_name=c.table_name AND occ.column_name=c.column_name
  LEFT JOIN col_dict cd
    ON cd.col_name = UPPER(c.column_name)
),

-- 8) COMMENT DDL 생성 (문자열 이스케이프 + 길이 절단)
tab_comment_ddl AS (
  SELECT
    ftc.owner, ftc.table_name, 2 AS seq,
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
    ) AS out_ddl
  FROM final_tab_cmt ftc
),
col_comment_ddl AS (
  SELECT
    fcc.owner, fcc.table_name, 3 AS seq, fcc.column_id,
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
    ) AS out_ddl
  FROM final_col_cmt fcc
)

-- 9) 최종 출력: CREATE → TABLE COMMENT → COLUMN COMMENT(컬럼순)
SELECT out_ddl
FROM (
  SELECT owner, table_name, seq, 0 AS col_order, out_ddl FROM create_ddl
  UNION ALL
  SELECT owner, table_name, seq, 0 AS col_order, out_ddl FROM tab_comment_ddl
  UNION ALL
  SELECT owner, table_name, seq, NVL(column_id,0) AS col_order, out_ddl FROM col_comment_ddl
)
ORDER BY owner, table_name, seq, col_order;
