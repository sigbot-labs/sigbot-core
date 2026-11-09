-- Isolated case bootstrap, not a substitute for runtime financial processing.
-- Opening capital is published as an external FundingFact and processed by the real wallet.
BEGIN;
INSERT INTO tenant_info(id,name) VALUES('0192f7b2-5c00-7000-8000-000000000001','Marketpulse tenant')
ON CONFLICT(id) DO NOTHING;
INSERT INTO workflow_info(id,env,tenant_id,name,status,dag_config,created_by,updated_by)
VALUES('0192f7b2-5c00-7000-8000-000000000002','e2e','0192f7b2-5c00-7000-8000-000000000001',
 'Marketpulse autonomy','RUNNING',
 '{"nodes":[{"id":1,"type":"Datafeed"},{"id":2,"type":"Datafeed"},{"id":3,"type":"Datafeed"},
 {"id":4,"type":"Evaluator"},{"id":5,"type":"Strategy"},{"id":6,"type":"Order"},{"id":8,"type":"Wallet"}],
 "connections":[{"source":1,"target":4},{"source":2,"target":4},{"source":3,"target":4},
 {"source":1,"target":5},{"source":2,"target":5},{"source":3,"target":5},{"source":4,"target":5},{"source":5,"target":6}]}',
 'marketpulse-e2e','marketpulse-e2e')
ON CONFLICT(id) DO NOTHING;
INSERT INTO ledger_info(id,tenant_id,account_id,exchange,mode,env)
VALUES('0192f7b2-5c00-7000-8000-000000000004','0192f7b2-5c00-7000-8000-000000000001',
 '0192f7b2-5c00-7000-8000-000000000003','BINANCE','paper','e2e')
ON CONFLICT(id) DO NOTHING;
COMMIT;
