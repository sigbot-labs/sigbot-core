-- Create database schema for wallet service
-- Wallet tables for trading system

-- s_wallets: 交易钱包账户表（生命周期：创建 → 永久；不包含资金数值）
CREATE TABLE IF NOT EXISTS s_wallets (
    wallet_id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id VARCHAR(255) NOT NULL,
    exchange VARCHAR(100) NOT NULL,
    mode VARCHAR(50) NOT NULL CHECK (mode IN ('live', 'backtest', 'paper')),
    account_type VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    del_flag BOOLEAN DEFAULT 0
);

-- s_ledger: 不可变账本，原始事实表（仅追加写入，不允许update/delete；回测 & 实盘统一）
CREATE TABLE IF NOT EXISTS s_ledger (
    wallet_id INTEGER NOT NULL REFERENCES s_wallets(wallet_id),
    order_id INTEGER NOT NULL,
    trade_id INTEGER NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    side VARCHAR(10) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price DECIMAL(30, 8) NOT NULL,
    qty DECIMAL(30, 8) NOT NULL,
    fee DECIMAL(30, 8) NOT NULL DEFAULT 0,
    fee_asset VARCHAR(20),
    exchange_order_id VARCHAR(255),
    exchange_trade_id VARCHAR(255),
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (wallet_id, order_id, trade_id)
);

-- s_balances: 派生状态（来源：SUM(trades) + funding + deposit - withdraw）
CREATE TABLE IF NOT EXISTS s_balances (
    wallet_id INTEGER NOT NULL REFERENCES s_wallets(wallet_id),
    asset VARCHAR(20) NOT NULL,
    available DECIMAL(30, 8) NOT NULL DEFAULT 0,
    locked DECIMAL(30, 8) NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (wallet_id, asset)
);

-- s_positions: 派生状态（来源：GROUP BY symbol + side FROM trades）
CREATE TABLE IF NOT EXISTS s_positions (
    wallet_id INTEGER NOT NULL REFERENCES s_wallets(wallet_id),
    symbol VARCHAR(50) NOT NULL,
    side VARCHAR(10) NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    size DECIMAL(30, 8) NOT NULL DEFAULT 0,
    entry_price DECIMAL(30, 8) NOT NULL DEFAULT 0,
    realized_pnl DECIMAL(30, 8) NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (wallet_id, symbol, side)
);

-- equity_snapshots: 资金快照（用于指标；可定期生成；不影响核心正确性）
CREATE TABLE IF NOT EXISTS equity_snapshots (
    wallet_id INTEGER NOT NULL REFERENCES s_wallets(wallet_id),
    equity DECIMAL(30, 8) NOT NULL,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_wallets_tenant_id ON s_wallets(tenant_id);
CREATE INDEX IF NOT EXISTS idx_wallets_exchange ON s_wallets(exchange);
CREATE INDEX IF NOT EXISTS idx_wallets_mode ON s_wallets(mode);
CREATE INDEX IF NOT EXISTS idx_trades_wallet_id ON s_ledger(wallet_id);
CREATE INDEX IF NOT EXISTS idx_trades_order_id ON s_ledger(order_id);
CREATE INDEX IF NOT EXISTS idx_trades_trade_id ON s_ledger(trade_id);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON s_ledger(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_ts ON s_ledger(ts);
CREATE INDEX IF NOT EXISTS idx_balances_wallet_id ON s_balances(wallet_id);
CREATE INDEX IF NOT EXISTS idx_positions_wallet_id ON s_positions(wallet_id);
CREATE INDEX IF NOT EXISTS idx_positions_symbol ON s_positions(symbol);
CREATE INDEX IF NOT EXISTS idx_equity_snapshots_wallet_id ON equity_snapshots(wallet_id);
CREATE INDEX IF NOT EXISTS idx_equity_snapshots_ts ON equity_snapshots(ts);
