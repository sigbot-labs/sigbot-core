INSERT INTO sys_tenant(id,name) VALUES(991001,'legacy migration acceptance');
        INSERT INTO t_workflow(id,name,flow_json,created_at,updated_at,del_flag) VALUES
        ('11111111-1111-4111-8111-111111111111','Legacy active workflow',
         '{"nodes":[{"id":"legacy-strategy","type":"strategy","data":{"name":"Legacy strategy"}}],"connections":[]}',
         '2025-01-21T01:02:03Z','2025-01-22T03:04:05Z',FALSE),
        ('22222222-2222-4222-8222-222222222222','Legacy deleted workflow','{"nodes":[],"connections":[]}',
         '2025-01-21T02:02:03Z','2025-01-22T04:04:05Z',TRUE);
        INSERT INTO t_workflow_run(id,workflow_id,params,start_date,end_date,run_type,state,profit,node_states) VALUES
        ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1','11111111-1111-4111-8111-111111111111',
         '{"period":"1h","legacy_param":1}','2025-02-01T00:00:00Z','2025-02-01T01:00:00Z','manual','success',
         '{"amount":12.34567890,"percentage":1.25}','{"legacy-strategy":"success"}'),
        ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2','22222222-2222-4222-8222-222222222222',
         '{"period":"5m"}','2025-02-02T00:00:00Z','2025-02-02T01:00:00Z','scheduled','failed',
         '{"amount":-0.125,"percentage":-0.1}','{"legacy-strategy":"failed"}'),
        ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3','11111111-1111-4111-8111-111111111111',
         '{"period":"1d"}','2025-02-03T00:00:00Z',NULL,'scheduled','running','{}','{}');
        INSERT INTO s_wallets(id,tenant_id,exchange,mode,account_type,created_at,updated_at,del_flag) VALUES
        (7000000001,'991001','BINANCE','live','spot','2025-07-16T00:00:00Z','2025-07-17T00:00:00Z',FALSE),
        (7000000002,'991001','IBKR','paper','margin','2025-07-16T00:00:00Z','2025-07-17T00:00:00Z',FALSE);
        INSERT INTO s_ledger(wallet_id,order_id,trade_id,symbol,side,price,qty,fee,fee_asset,exchange_order_id,exchange_trade_id,ts) VALUES
        (7000000001,101,201,'BTCUSDT','BUY',60000.12345678,0.12345678,0.00001234,'BNB','legacy-order-1','legacy-trade-1','2025-07-16T01:00:00Z'),
        (7000000001,102,202,'ETHUSDT','SELL',3000.87654321,2.50000000,0.75000000,'USDT','legacy-order-2','legacy-trade-2','2025-07-16T02:00:00Z'),
        (7000000002,103,203,'AAPL','BUY',210.12500000,10.00000000,1.25000000,'USD','legacy-order-3','legacy-trade-3','2025-07-16T03:00:00Z');
        INSERT INTO s_balances(wallet_id,asset,available,locked,updated_at) VALUES
        (7000000001,'USDT',1234567890123456789012.12345678,99.87654321,'2025-07-16T04:00:00Z'),
        (7000000001,'BTC',0.12345678,0.00000001,'2025-07-16T04:00:00Z'),
        (7000000002,'USD',98765.43210000,1234.56780000,'2025-07-16T04:00:00Z');
        INSERT INTO s_positions(wallet_id,symbol,side,size,entry_price,realized_pnl,updated_at) VALUES
        (7000000001,'BTCUSDT','LONG',0.12345678,60000.12345678,-1.12345678,'2025-07-16T04:00:00Z'),
        (7000000001,'ETHUSDT','SHORT',2.50000000,3000.87654321,12.34567890,'2025-07-16T04:00:00Z'),
        (7000000002,'AAPL','LONG',10.00000000,210.12500000,0.00000000,'2025-07-16T04:00:00Z');
        INSERT INTO equity_snapshots(wallet_id,equity,ts) VALUES
        (7000000001,1234567890123456789012.12345678,'2025-07-16T05:00:00Z'),
        (7000000002,100000.12500000,'2025-07-16T05:00:00Z');
