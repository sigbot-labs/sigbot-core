BEGIN;
INSERT INTO ledger_info(id,tenant_id,exchange,mode,env,account_id)
VALUES('0192f7b2-5c00-7000-8000-000000000104',
       '0192f7b2-5c00-7000-8000-000000000101',
       'BINANCE','paper','migration-test',
       '0192f7b2-5c00-7000-8000-000000000103');
INSERT INTO wlt_journal(wallet_id,producer_node_id,run_id,seq,source,fact_ref,occurred_at)
VALUES('0192f7b2-5c00-7000-8000-000000000104',1,
       '0192f7b2-5c00-7000-8000-000000000102',0,'funding','balanced',NOW());
INSERT INTO wlt_posting(wallet_id,producer_node_id,run_id,seq,line_no,account,asset,amount)
VALUES('0192f7b2-5c00-7000-8000-000000000104',1,
       '0192f7b2-5c00-7000-8000-000000000102',0,0,'wallet','USDT',100),
      ('0192f7b2-5c00-7000-8000-000000000104',1,
       '0192f7b2-5c00-7000-8000-000000000102',0,1,'clearing','USDT',-100);
SET CONSTRAINTS ALL IMMEDIATE;

DO $test$
DECLARE rejected BOOLEAN := FALSE;
BEGIN
    BEGIN
        UPDATE wlt_posting SET amount=101
        WHERE wallet_id='0192f7b2-5c00-7000-8000-000000000104' AND line_no=0;
    EXCEPTION WHEN raise_exception THEN rejected := TRUE;
    END;
    IF NOT rejected THEN RAISE EXCEPTION 'immutable posting was changed'; END IF;
END $test$;

DO $test$
DECLARE rejected BOOLEAN := FALSE;
BEGIN
    BEGIN
        DELETE FROM wlt_journal
        WHERE wallet_id='0192f7b2-5c00-7000-8000-000000000104';
    EXCEPTION WHEN raise_exception THEN rejected := TRUE;
    END;
    IF NOT rejected THEN RAISE EXCEPTION 'immutable journal was deleted'; END IF;
END $test$;

SET CONSTRAINTS ALL DEFERRED;
DO $test$
DECLARE rejected BOOLEAN := FALSE;
BEGIN
    BEGIN
        INSERT INTO wlt_journal(wallet_id,producer_node_id,run_id,seq,source,fact_ref,occurred_at)
        VALUES('0192f7b2-5c00-7000-8000-000000000104',1,
               '0192f7b2-5c00-7000-8000-000000000102',1,'funding','unbalanced',NOW());
        INSERT INTO wlt_posting(wallet_id,producer_node_id,run_id,seq,line_no,account,asset,amount)
        VALUES('0192f7b2-5c00-7000-8000-000000000104',1,
               '0192f7b2-5c00-7000-8000-000000000102',1,0,'wallet','USDT',1);
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN raise_exception THEN rejected := TRUE;
    END;
    IF NOT rejected THEN RAISE EXCEPTION 'unbalanced journal committed'; END IF;
END $test$;

DO $test$
DECLARE rejected BOOLEAN := FALSE;
BEGIN
    BEGIN
        INSERT INTO wlt_journal(wallet_id,producer_node_id,run_id,seq,source,fact_ref,occurred_at)
        VALUES('0192f7b2-5c00-7000-8000-000000000104',1,
               '0192f7b2-5c00-7000-8000-000000000102',2,'funding','empty',NOW());
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN raise_exception THEN rejected := TRUE;
    END;
    IF NOT rejected THEN RAISE EXCEPTION 'journal without postings committed'; END IF;
END $test$;

ROLLBACK;
