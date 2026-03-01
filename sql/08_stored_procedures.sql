-- =============================================================================
-- Phase H: ストアドプロシージャの作成
-- =============================================================================
-- GCS から Bronze テーブルにデータをロードするためのプロシージャ。
--
-- 【学べること】
-- - Snowflake Scripting（SQL ベースのプロシージャ）
-- - COPY INTO: 外部ステージからテーブルへのバルクロード
-- - FULL ロード（TRUNCATE + COPY）と DAILY ロード（追記）の使い分け
-- - 動的 SQL の組み立てとパスパターンの生成
-- =============================================================================

-- LOAD_COLLECTION: 日付から GCS パスを動的に組み立て、COPY INTO を実行
CREATE OR REPLACE PROCEDURE BRONZE.LOAD_COLLECTION(
    COLLECTION_NAME VARCHAR,
    TARGET_TABLE    VARCHAR,
    LOAD_TYPE       VARCHAR,
    TARGET_DATE     DATE
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_ym  VARCHAR;
    v_ymd VARCHAR;
    v_path VARCHAR;
    v_sql  VARCHAR;
BEGIN
    v_ym  := TO_CHAR(:TARGET_DATE, 'YYYYMM');
    v_ymd := TO_CHAR(:TARGET_DATE, 'YYYYMMDD');
    v_path := 'ym=' || v_ym || '/ymd=' || v_ymd || '/' || :COLLECTION_NAME || '.json.gz';

    IF (:LOAD_TYPE = 'FULL') THEN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || :TARGET_TABLE;
        v_sql := '
            COPY INTO ' || :TARGET_TABLE || ' (RAW_DATA, LOAD_DATE)
            FROM (
                SELECT $1, ''' || :TARGET_DATE::VARCHAR || '''::DATE
                FROM @BRONZE.GCS_RAW_DATA/' || v_path || '
            )
            ON_ERROR = ''ABORT_STATEMENT''
        ';
        EXECUTE IMMEDIATE v_sql;
    ELSE
        v_sql := '
            COPY INTO ' || :TARGET_TABLE || ' (RAW_DATA, LOAD_DATE)
            FROM (
                SELECT $1, ''' || :TARGET_DATE::VARCHAR || '''::DATE
                FROM @BRONZE.GCS_RAW_DATA/' || v_path || '
            )
            ON_ERROR = ''ABORT_STATEMENT''
        ';
        EXECUTE IMMEDIATE v_sql;
    END IF;

    INSERT INTO MONITORING.LOAD_LOG (COLLECTION_NAME, LOAD_TYPE, LOAD_DATE, STATUS, EXECUTED_AT)
    VALUES (:COLLECTION_NAME, :LOAD_TYPE, :TARGET_DATE, 'SUCCESS', CURRENT_TIMESTAMP());

    RETURN 'OK: ' || :COLLECTION_NAME || ' (' || :LOAD_TYPE || ') for ' || :TARGET_DATE::VARCHAR;
END;
$$;

-- LOAD_ALL: 3つのコレクションをまとめてロードするラッパー
CREATE OR REPLACE PROCEDURE BRONZE.LOAD_ALL(TARGET_DATE DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CALL BRONZE.LOAD_COLLECTION('members', 'BRONZE.MEMBERS', 'FULL', :TARGET_DATE);
    CALL BRONZE.LOAD_COLLECTION('transactions', 'BRONZE.TRANSACTIONS', 'DAILY', :TARGET_DATE);
    CALL BRONZE.LOAD_COLLECTION('daily_member_summary', 'BRONZE.DAILY_MEMBER_SUMMARY', 'DAILY', :TARGET_DATE);
    RETURN 'ALL LOADS COMPLETED for ' || :TARGET_DATE::VARCHAR;
END;
$$;
