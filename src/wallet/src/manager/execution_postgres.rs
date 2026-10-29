// SPDX-License-Identifier: GPL-3.0-only
//! The wallet is the sole writer of financial facts and portfolio projections.

use anyhow::{ensure, Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sigbot_core::modules::wallet::store::transaction::{
    projection::{PositionProjection, TradeProjection},
    reservation_postgres::PostgresWalletReservation,
};
use sigbot_messager::storage::postgres::PostgresMessageStore;
use sigbot_types::modules::{
    messager::{
        envelope::{DurableMessage, MessageEnvelope, MessageKind},
        topic::{AccountTopic, MessageScope},
    },
    order::execution::{ExecutionFact, OrderState, OrderStatusFact},
    wallet::funding::FundingFact,
};
use sqlx::{PgPool, Postgres, Row, Transaction};

pub struct WalletExecutionProcessor {
    pool: PgPool,
    scope: MessageScope,
    account_id: i64,
    node_id: i64,
}

impl WalletExecutionProcessor {
    pub fn producer(&self) -> String {
        format!(
            "wallet:{}:{}:{}:{}:{}",
            self.scope.env, self.scope.tenant_id, self.scope.space_id, self.account_id, self.node_id
        )
    }
    pub fn new(pool: PgPool, scope: MessageScope, account_id: i64, node_id: i64) -> Result<Self> {
        scope.validate()?;
        ensure!(account_id > 0 && node_id > 0, "wallet account/node must be positive");
        Ok(Self {
            pool,
            scope,
            account_id,
            node_id,
        })
    }

    pub async fn process_funding(&self, message: &MessageEnvelope<FundingFact>, traceparent: String) -> Result<bool> {
        message.validate()?;
        let input = &message.durable;
        let fact = &input.payload;
        fact.validate()?;
        ensure!(input.available_time <= Utc::now(), "funding fact is not yet available");
        ensure!(
            input.scope == self.scope
                && input.account_id == Some(self.account_id)
                && fact.account_id == self.account_id,
            "funding isolation scope mismatch"
        );
        ensure!(
            input.kind == MessageKind::Event && input.event_time == fact.occurred_at,
            "funding requires an observed fact"
        );
        let mut tx = self.pool.begin().await?;
        let wallet = sqlx::query(
            "SELECT id FROM s_wallets WHERE id=$1 AND tenant_id=$2 AND env=$3 AND space_id=$4
            AND exchange=$5 AND account_id=$6 FOR UPDATE",
        )
        .bind(fact.wallet_id)
        .bind(self.scope.tenant_id.to_string())
        .bind(&self.scope.env)
        .bind(self.scope.space_id)
        .bind(&fact.exchange)
        .bind(self.account_id)
        .fetch_optional(&mut *tx)
        .await?;
        ensure!(wallet.is_some(), "funding wallet is outside this account");
        let topic = AccountTopic::ExecutionEvent.topic(&self.scope, self.account_id)?;
        let wire: MessageEnvelope<Value> = serde_json::from_value(serde_json::to_value(message)?)?;
        let consumer = format!("wallet.funding.{}", self.node_id);
        if !PostgresMessageStore::accept(&mut tx, &consumer, &topic, &wire).await? {
            tx.commit().await?;
            return Ok(false);
        }
        let source = format!("funding/{}", fact.source);
        let payload = serde_json::to_value(fact)?;
        let inserted = sqlx::query(
            "INSERT INTO wallet_journal(wallet_id,run_id,seq,source,fact_ref,occurred_at,payload,producer_node_id)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING",
        )
        .bind(fact.wallet_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .bind(&source)
        .bind(&fact.source_event_id)
        .bind(fact.occurred_at)
        .bind(&payload)
        .bind(input.node_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if inserted == 1 {
            for (line, account, amount) in [(0, "available", fact.amount), (1, "clearing", -fact.amount)] {
                sqlx::query("INSERT INTO wallet_posting(wallet_id,run_id,seq,line_no,account,asset,amount,producer_node_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
                    .bind(fact.wallet_id).bind(&input.run_id).bind(input.seq).bind(line).bind(account).bind(&fact.asset).bind(amount).bind(input.node_id)
                    .execute(&mut *tx).await?;
            }
            sqlx::query("INSERT INTO s_balances(wallet_id,asset,available,locked) VALUES($1,$2,$3,0)
                ON CONFLICT(wallet_id,asset) DO UPDATE SET available=s_balances.available+EXCLUDED.available,updated_at=NOW()")
                .bind(fact.wallet_id).bind(&fact.asset).bind(fact.amount).execute(&mut *tx).await?;
            self.publish_portfolio(&mut tx, &wire.durable, fact.wallet_id, traceparent)
                .await?;
        } else {
            let previous: Option<Value> = sqlx::query_scalar(
                "SELECT payload FROM wallet_journal WHERE wallet_id=$1 AND source=$2 AND fact_ref=$3",
            )
            .bind(fact.wallet_id)
            .bind(source)
            .bind(&fact.source_event_id)
            .fetch_optional(&mut *tx)
            .await?;
            ensure!(
                previous == Some(payload),
                "funding identity reused with different facts"
            );
        }
        PostgresMessageStore::complete(&mut tx, &consumer, &wire.durable).await?;
        tx.commit().await?;
        Ok(inserted == 1)
    }

    /// Opening balances must first be represented by an immutable funding journal.
    /// Reservations do not change asset ownership: available = total - remaining locks.
    pub async fn rebuild_balances(&self, wallet_id: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let exists = sqlx::query("SELECT id FROM s_wallets WHERE id=$1 AND env=$2 AND tenant_id=$3 AND space_id=$4 AND account_id=$5 FOR UPDATE")
            .bind(wallet_id).bind(&self.scope.env).bind(self.scope.tenant_id.to_string()).bind(self.scope.space_id)
            .bind(self.account_id).fetch_optional(&mut *tx).await?;
        ensure!(exists.is_some(), "wallet cannot be rebuilt outside its account");
        let journaled: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM wallet_journal WHERE wallet_id=$1)")
            .bind(wallet_id)
            .fetch_one(&mut *tx)
            .await?;
        ensure!(journaled, "rebuild requires journaled opening balances");
        sqlx::query("DELETE FROM s_balances WHERE wallet_id=$1")
            .bind(wallet_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO s_balances(wallet_id,asset,available,locked)
            WITH totals AS (SELECT asset,SUM(amount) amount FROM wallet_posting WHERE wallet_id=$1 AND account='available' GROUP BY asset),
            reserves AS (SELECT asset,SUM(remaining) amount FROM wallet_reservation WHERE wallet_id=$1 AND state='reserved' GROUP BY asset)
            SELECT $1,COALESCE(t.asset,r.asset),COALESCE(t.amount,0)-COALESCE(r.amount,0),COALESCE(r.amount,0)
            FROM totals t FULL OUTER JOIN reserves r ON t.asset=r.asset")
            .bind(wallet_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// producer_traceparent is the current wallet producer span, supplied by the host.
    pub async fn process(
        &self,
        message: &MessageEnvelope<ExecutionFact>,
        producer_traceparent: String,
    ) -> Result<bool> {
        message.validate()?;
        let input = &message.durable;
        let fact = &input.payload;
        ensure!(
            input.available_time <= Utc::now(),
            "execution fact is not yet available"
        );
        ensure!(
            input.scope == self.scope
                && input.account_id == Some(self.account_id)
                && fact.account_id == self.account_id,
            "wallet execution isolation scope mismatch"
        );
        ensure!(
            input.kind == MessageKind::Event && fact.executed_at == input.event_time,
            "execution must be a fact with its actual event time"
        );
        let topic = AccountTopic::ExecutionEvent.topic(&self.scope, self.account_id)?;
        let wire: MessageEnvelope<Value> = serde_json::from_value(serde_json::to_value(message)?)?;
        let mut tx = self.pool.begin().await?;
        let wallet = sqlx::query(
            "SELECT portfolio_version FROM s_wallets WHERE id=$1 AND tenant_id=$2
            AND env=$3 AND space_id=$4 AND exchange=$5 AND account_id=$6 FOR UPDATE",
        )
        .bind(fact.wallet_id)
        .bind(self.scope.tenant_id.to_string())
        .bind(&self.scope.env)
        .bind(self.scope.space_id)
        .bind(&fact.exchange)
        .bind(self.account_id)
        .fetch_optional(&mut *tx)
        .await?;
        ensure!(wallet.is_some(), "execution wallet does not belong to this account");
        let consumer = format!("wallet.execution.{}", self.node_id);
        if !PostgresMessageStore::accept(&mut tx, &consumer, &topic, &wire).await? {
            tx.commit().await?;
            return Ok(false);
        }
        fact.validate()?;
        let payload = serde_json::to_value(fact)?;
        let inserted = sqlx::query(
            "INSERT INTO wallet_journal(wallet_id,run_id,seq,source,fact_ref,occurred_at,payload,producer_node_id)
            VALUES($1,$2,$3,'execution',$4,$5,$6,$7) ON CONFLICT DO NOTHING",
        )
        .bind(fact.wallet_id)
        .bind(&input.run_id)
        .bind(input.seq)
        .bind(&fact.exchange_trade_id)
        .bind(fact.executed_at)
        .bind(&payload)
        .bind(input.node_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if inserted == 0 {
            let prior = sqlx::query_scalar::<_, Value>(
                "SELECT payload FROM wallet_journal WHERE wallet_id=$1
                AND source='execution' AND fact_ref=$2",
            )
            .bind(fact.wallet_id)
            .bind(&fact.exchange_trade_id)
            .fetch_optional(&mut *tx)
            .await?;
            ensure!(
                prior == Some(payload),
                "execution identity was reused with different facts"
            );
            PostgresMessageStore::complete(&mut tx, &consumer, &wire.durable).await?;
            tx.commit().await?;
            return Ok(false);
        }
        let projection = TradeProjection::from_execution(fact)?;
        Self::settle(&mut tx, input, &projection).await?;
        Self::post(&mut tx, input, &projection).await?;
        Self::release_completed(&mut tx, input).await?;
        self.release_pending_statuses(&mut tx, fact.wallet_id).await?;
        Self::rebuild_position(&mut tx, fact).await?;
        self.publish_portfolio(&mut tx, &wire.durable, fact.wallet_id, producer_traceparent)
            .await?;
        PostgresMessageStore::complete(&mut tx, &consumer, &wire.durable).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Terminal venue status may precede its fills. Persist it and acknowledge
    /// intake without blocking the execution subscription that will finish it.
    pub async fn process_status(
        &self,
        message: &MessageEnvelope<OrderStatusFact>,
        traceparent: String,
    ) -> Result<bool> {
        message.validate()?;
        let input = &message.durable;
        let fact = &input.payload;
        fact.validate()?;
        ensure!(
            input.scope == self.scope
                && input.account_id == Some(self.account_id)
                && fact.account_id == self.account_id
                && input.kind == MessageKind::Event
                && input.event_time == fact.occurred_at
                && input.available_time <= Utc::now(),
            "wallet status scope or time mismatch"
        );
        let mut tx = self.pool.begin().await?;
        let owned: Option<i64> = sqlx::query_scalar("SELECT id FROM s_wallets WHERE id=$1 AND account_id=$2 AND env=$3 AND tenant_id=$4 AND space_id=$5 AND exchange=$6 FOR UPDATE")
            .bind(fact.wallet_id).bind(self.account_id).bind(&self.scope.env).bind(self.scope.tenant_id.to_string()).bind(self.scope.space_id)
            .bind(&fact.exchange).fetch_optional(&mut *tx).await?;
        ensure!(owned.is_some(), "terminal status wallet is outside its account");
        let consumer = format!("{}.status", self.producer());
        let wire: MessageEnvelope<Value> = serde_json::from_value(serde_json::to_value(message)?)?;
        if !PostgresMessageStore::accept(
            &mut tx,
            &consumer,
            &AccountTopic::OrderEvent.topic(&self.scope, self.account_id)?,
            &wire,
        )
        .await?
        {
            tx.commit().await?;
            return Ok(false);
        }
        if let Some(changed) = Self::apply_status(&mut tx, input).await? {
            if changed {
                self.publish_portfolio(&mut tx, &wire.durable, fact.wallet_id, traceparent)
                    .await?;
            }
            PostgresMessageStore::complete(&mut tx, &consumer, &wire.durable).await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    async fn release_pending_statuses(&self, tx: &mut Transaction<'_, Postgres>, wallet_id: i64) -> Result<()> {
        let consumer = format!("{}.status", self.producer());
        let pending: Vec<Value> = sqlx::query_scalar(
            "SELECT payload FROM messager_inbox WHERE consumer=$1 AND env=$2 AND tenant_id=$3 AND space_id=$4
            AND state='pending' AND (payload->'payload'->>'wallet_id')::bigint=$5 ORDER BY received_at LIMIT 32",
        )
        .bind(&consumer)
        .bind(&self.scope.env)
        .bind(self.scope.tenant_id)
        .bind(self.scope.space_id)
        .bind(wallet_id)
        .fetch_all(&mut **tx)
        .await?;
        for value in pending {
            let status: DurableMessage<OrderStatusFact> = serde_json::from_value(value)?;
            if Self::apply_status(tx, &status).await?.is_some() {
                let value = serde_json::to_value(&status.payload)?;
                PostgresMessageStore::complete(tx, &consumer, &status.map_payload(|_| value)).await?;
            }
        }
        Ok(())
    }

    async fn apply_status(
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<OrderStatusFact>,
    ) -> Result<Option<bool>> {
        let fact = &input.payload;
        if !matches!(
            fact.receipt.state,
            OrderState::Filled | OrderState::Cancelled | OrderState::Rejected
        ) {
            return Ok(Some(false));
        }
        let intent: Value = sqlx::query_scalar(
            "SELECT intent FROM order_record WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4",
        )
        .bind(fact.wallet_id)
        .bind(fact.reservation_node_id)
        .bind(&input.run_id)
        .bind(fact.reservation_seq)
        .fetch_one(&mut **tx)
        .await?;
        let intent: sigbot_types::modules::order::intent::TradeIntent = serde_json::from_value(intent)?;
        ensure!(
            fact.executed_quantity <= intent.quantity
                && (fact.receipt.state != OrderState::Filled || fact.executed_quantity == intent.quantity),
            "terminal status has inconsistent cumulative quantity"
        );
        let booked: Decimal = sqlx::query_scalar("SELECT COALESCE(SUM((payload->>'quantity')::numeric),0) FROM wallet_journal WHERE wallet_id=$1
            AND source='execution' AND run_id=$2 AND (payload->>'reservation_node_id')::bigint=$3 AND (payload->>'reservation_seq')::bigint=$4")
            .bind(fact.wallet_id).bind(&input.run_id).bind(fact.reservation_node_id).bind(fact.reservation_seq).fetch_one(&mut **tx).await?;
        ensure!(
            booked <= fact.executed_quantity,
            "terminal status omits executions; reconciliation required"
        );
        if booked < fact.executed_quantity {
            return Ok(None);
        }
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM wallet_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4)")
            .bind(fact.wallet_id).bind(fact.reservation_node_id).bind(&input.run_id).bind(fact.reservation_seq).fetch_one(&mut **tx).await?;
        if !exists {
            return Ok(Some(false));
        }
        Ok(Some(
            PostgresWalletReservation::release_in(
                tx,
                fact.wallet_id,
                fact.reservation_node_id,
                &input.run_id,
                fact.reservation_seq,
            )
            .await?,
        ))
    }

    pub(crate) async fn publish_portfolio(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<Value>,
        wallet_id: i64,
        producer_traceparent: String,
    ) -> Result<()> {
        let version: i64 = sqlx::query_scalar(
            "UPDATE s_wallets SET portfolio_version=portfolio_version+1 WHERE id=$1 RETURNING portfolio_version",
        )
        .bind(wallet_id)
        .fetch_one(&mut **tx)
        .await?;
        let balances = sqlx::query("SELECT asset,available,locked FROM s_balances WHERE wallet_id=$1 ORDER BY asset")
            .bind(wallet_id)
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .map(|row| {
                json!({
                "asset":row.get::<String,_>("asset"),"available":row.get::<Decimal,_>("available"),
                "locked":row.get::<Decimal,_>("locked") })
            })
            .collect::<Vec<_>>();
        let positions = sqlx::query(
            "SELECT symbol,side,size,entry_price,realized_pnl FROM s_positions WHERE wallet_id=$1 ORDER BY symbol,side",
        )
        .bind(wallet_id)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(|row| {
            json!({
                "symbol":row.get::<String,_>("symbol"),"side":row.get::<String,_>("side"),
                "size":row.get::<Decimal,_>("size"),"entry_price":row.get::<Decimal,_>("entry_price"),
                "realized_pnl":row.get::<Decimal,_>("realized_pnl") })
        })
        .collect::<Vec<_>>();
        let seq = PostgresMessageStore::next_sequence(tx, &self.scope, self.node_id, &input.run_id).await?;
        let portfolio = DurableMessage {
            version: 2,
            scope: self.scope.clone(),
            workflow_id: input.workflow_id,
            account_id: Some(self.account_id),
            node_id: self.node_id,
            target_node_id: None,
            run_id: input.run_id.clone(),
            seq,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: input.event_time,
            available_time: input.available_time,
            produced_at: Utc::now(),
            traceparent: producer_traceparent,
            payload: json!({"version":version,"wallet_id":wallet_id,"balances":balances,"positions":positions}),
        };
        let output_topic = AccountTopic::PortfolioSnapshot.topic(&self.scope, self.account_id)?;
        PostgresMessageStore::enqueue(tx, &self.producer(), &output_topic, &portfolio).await?;
        Ok(())
    }

    async fn settle(
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<ExecutionFact>,
        projection: &TradeProjection,
    ) -> Result<()> {
        let fact = &input.payload;
        let asset: String = sqlx::query_scalar(
            "SELECT asset FROM wallet_reservation WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4",
        )
        .bind(fact.wallet_id)
        .bind(fact.reservation_node_id)
        .bind(&input.run_id)
        .bind(fact.reservation_seq)
        .fetch_one(&mut **tx)
        .await
        .context("execution requires its original wallet reservation; reconcile missing facts")?;
        let (expected, mut spent) = if projection.buy {
            (&projection.quote_asset, projection.quote_quantity)
        } else {
            (&projection.base_asset, fact.quantity)
        };
        ensure!(&asset == expected, "reservation asset differs from execution debit");
        if fact.fee_asset == asset {
            spent = spent.checked_add(fact.fee).context("fee overflow")?;
        }
        PostgresWalletReservation::settle_in(
            tx,
            fact.wallet_id,
            fact.reservation_node_id,
            &input.run_id,
            fact.reservation_seq,
            spent,
        )
        .await
    }

    async fn release_completed(
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<ExecutionFact>,
    ) -> Result<()> {
        let fact = &input.payload;
        let state = sqlx::query(
            "SELECT COALESCE(BOOL_OR((payload->>'order_complete')::boolean),false) complete,
            COALESCE(SUM((payload->>'quantity')::numeric),0) quantity FROM wallet_journal
            WHERE wallet_id=$1 AND source='execution' AND payload->>'exchange_order_id'=$2",
        )
        .bind(fact.wallet_id)
        .bind(&fact.exchange_order_id)
        .fetch_one(&mut **tx)
        .await?;
        if state.get::<bool, _>("complete") {
            let expected: Decimal = sqlx::query_scalar(
                "SELECT (intent->>'quantity')::numeric FROM order_record
                WHERE wallet_id=$1 AND producer_node_id=$2 AND run_id=$3 AND seq=$4",
            )
            .bind(fact.wallet_id)
            .bind(fact.reservation_node_id)
            .bind(&input.run_id)
            .bind(fact.reservation_seq)
            .fetch_one(&mut **tx)
            .await
            .context("terminal execution requires its persisted intent")?;
            let filled: Decimal = state.get("quantity");
            ensure!(filled <= expected, "exchange overfill requires reconciliation");
            // A terminal fill may arrive before earlier partials. Release only once
            // every observed fill accounts for the intended quantity.
            if filled == expected {
                PostgresWalletReservation::release_in(
                    tx,
                    fact.wallet_id,
                    fact.reservation_node_id,
                    &input.run_id,
                    fact.reservation_seq,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn post(
        tx: &mut Transaction<'_, Postgres>,
        input: &DurableMessage<ExecutionFact>,
        projection: &TradeProjection,
    ) -> Result<()> {
        for (index, (asset, amount, counterparty)) in projection.movements()?.into_iter().enumerate() {
            for (offset, account, amount) in [(0, "available", amount), (1, counterparty, -amount)] {
                sqlx::query("INSERT INTO wallet_posting(wallet_id,run_id,seq,line_no,account,asset,amount,producer_node_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
                    .bind(input.payload.wallet_id).bind(&input.run_id).bind(input.seq).bind((index * 2 + offset) as i32)
                    .bind(account).bind(&asset).bind(amount).bind(input.node_id).execute(&mut **tx).await?;
            }
        }
        for (asset, delta) in projection.balance_deltas()? {
            sqlx::query("INSERT INTO s_balances(wallet_id,asset,available,locked) VALUES($1,$2,$3,0)
                ON CONFLICT(wallet_id,asset) DO UPDATE SET available=s_balances.available+EXCLUDED.available,updated_at=NOW()")
                .bind(input.payload.wallet_id).bind(asset).bind(delta).execute(&mut **tx).await?;
        }
        Ok(())
    }

    async fn rebuild_position(tx: &mut Transaction<'_, Postgres>, fact: &ExecutionFact) -> Result<()> {
        let history = sqlx::query(
            "SELECT j.payload,
            (SELECT p.asset FROM wallet_posting p WHERE p.wallet_id=j.wallet_id AND p.producer_node_id=j.producer_node_id AND p.run_id=j.run_id AND p.seq=j.seq AND p.line_no=0) base_asset,
            (SELECT p.asset FROM wallet_posting p WHERE p.wallet_id=j.wallet_id AND p.producer_node_id=j.producer_node_id AND p.run_id=j.run_id AND p.seq=j.seq AND p.line_no=2) quote_asset
            FROM wallet_journal j WHERE wallet_id=$1
            AND source='execution' AND payload->>'symbol'=$2 ORDER BY occurred_at,fact_ref",
        )
        .bind(fact.wallet_id)
        .bind(&fact.symbol)
        .fetch_all(&mut **tx)
        .await?;
        let mut position = PositionProjection::default();
        for row in history {
            let mut execution: ExecutionFact = serde_json::from_value(row.get("payload"))?;
            // Earlier v2 facts did not carry instrument metadata. Their immutable
            // postings name the assets; migration never guesses a symbol.
            if execution.base_asset.is_empty() {
                execution.base_asset = row
                    .get::<Option<String>, _>("base_asset")
                    .context("legacy execution has no base-asset posting")?;
            }
            if execution.quote_asset.is_empty() {
                execution.quote_asset = row
                    .get::<Option<String>, _>("quote_asset")
                    .context("legacy execution has no quote-asset posting")?;
            }
            let fill = TradeProjection::from_execution(&execution)?;
            position.apply_quote(fill.buy, fill.price, fill.quantity, fill.quote_quantity)?;
        }
        sqlx::query("DELETE FROM s_positions WHERE wallet_id=$1 AND symbol=$2")
            .bind(fact.wallet_id)
            .bind(&fact.symbol)
            .execute(&mut **tx)
            .await?;
        sqlx::query(
            "INSERT INTO s_positions(wallet_id,symbol,side,size,entry_price,realized_pnl)
            VALUES($1,$2,$3,$4,$5,$6)",
        )
        .bind(fact.wallet_id)
        .bind(&fact.symbol)
        .bind(if position.size.is_sign_negative() {
            "SHORT"
        } else {
            "LONG"
        })
        .bind(position.size.abs())
        .bind(position.entry_price)
        .bind(position.realized_pnl)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}
