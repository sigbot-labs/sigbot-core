-- Isolated case bootstrap, not a substitute for runtime financial processing.
-- Opening capital is published as an external FundingFact and processed by the real wallet.
BEGIN;
INSERT INTO sys_tenant(id,name) VALUES(1,'Marketpulse E2E') ON CONFLICT(id) DO NOTHING;
INSERT INTO s_workflow(id,env,tenant_id,space_id,name,status,flow_json)
VALUES(10001,'e2e',1,1,'Marketpulse autonomy','RUNNING',
 '{"nodes":[{"id":1,"type":"Datafeed"},{"id":2,"type":"Datafeed"},{"id":3,"type":"Datafeed"},
 {"id":4,"type":"Evaluator"},{"id":5,"type":"Strategy"},{"id":6,"type":"Order"},{"id":8,"type":"Wallet"}],
 "connections":[{"source":1,"target":4},{"source":2,"target":4},{"source":3,"target":4},
 {"source":1,"target":5},{"source":2,"target":5},{"source":3,"target":5},{"source":4,"target":5},{"source":5,"target":6}]}')
ON CONFLICT(id) DO NOTHING;
INSERT INTO s_wallets(id,tenant_id,account_id,exchange,mode,env,space_id)
VALUES(1,'1',1,'BINANCE','backtest','e2e',1) ON CONFLICT(id) DO NOTHING;
SELECT setval(pg_get_serial_sequence('s_workflow','id'), (SELECT GREATEST(MAX(id),1) FROM s_workflow), true);
SELECT setval(pg_get_serial_sequence('s_wallets','id'), (SELECT GREATEST(MAX(id),1) FROM s_wallets), true);
COMMIT;
