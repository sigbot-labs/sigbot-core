\set ON_ERROR_STOP on
-- Execute against an isolated migrated PostgreSQL database. All fixture writes roll back.
BEGIN;
INSERT INTO s_wallets(id,tenant_id,exchange,mode,env,space_id,account_id)
VALUES(9000000001,'9000000001','binance','paper','migration-test',1,9000000001);
INSERT INTO wallet_journal(wallet_id,producer_node_id,run_id,seq,source,fact_ref,occurred_at)
VALUES(9000000001,1,'migration-test',0,'funding','balanced',NOW());
INSERT INTO wallet_posting(wallet_id,producer_node_id,run_id,seq,line_no,account,asset,amount)
VALUES(9000000001,1,'migration-test',0,0,'wallet','USDT',100),
      (9000000001,1,'migration-test',0,1,'clearing','USDT',-100);
SET CONSTRAINTS ALL IMMEDIATE;

DO $test$
DECLARE rejected BOOLEAN := FALSE;
BEGIN
    BEGIN
        UPDATE wallet_posting SET amount=101 WHERE wallet_id=9000000001 AND line_no=0;
    EXCEPTION WHEN raise_exception THEN rejected := TRUE;
    END;
    IF NOT rejected THEN RAISE EXCEPTION 'immutable posting was changed'; END IF;
END $test$;

DO $test$
DECLARE rejected BOOLEAN := FALSE;
BEGIN
    BEGIN
        DELETE FROM wallet_journal WHERE wallet_id=9000000001;
    EXCEPTION WHEN raise_exception THEN rejected := TRUE;
    END;
    IF NOT rejected THEN RAISE EXCEPTION 'immutable journal was deleted'; END IF;
END $test$;

SET CONSTRAINTS ALL DEFERRED;
DO $test$
DECLARE rejected BOOLEAN := FALSE;
BEGIN
    BEGIN
        INSERT INTO wallet_journal(wallet_id,producer_node_id,run_id,seq,source,fact_ref,occurred_at)
        VALUES(9000000001,1,'migration-test',1,'funding','unbalanced',NOW());
        INSERT INTO wallet_posting(wallet_id,producer_node_id,run_id,seq,line_no,account,asset,amount)
        VALUES(9000000001,1,'migration-test',1,0,'wallet','USDT',1);
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN raise_exception THEN rejected := TRUE;
    END;
    IF NOT rejected THEN RAISE EXCEPTION 'unbalanced journal committed'; END IF;
END $test$;

DO $test$
DECLARE rejected BOOLEAN := FALSE;
BEGIN
    BEGIN
        INSERT INTO wallet_journal(wallet_id,producer_node_id,run_id,seq,source,fact_ref,occurred_at)
        VALUES(9000000001,1,'migration-test',2,'funding','empty',NOW());
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN raise_exception THEN rejected := TRUE;
    END;
    IF NOT rejected THEN RAISE EXCEPTION 'journal without postings committed'; END IF;
END $test$;

ROLLBACK;
