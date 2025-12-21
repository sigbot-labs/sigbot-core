// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use crate::config::config::MongoAppDBProperties;
use crate::modules::wallet::store::transaction::IWalletUpdater;
use crate::store::mongo::MongoRepository;
use anyhow::{Context, Error};
use async_trait::async_trait;
use common_telemetry::{error, info};
use futures::stream::TryStreamExt;
use mongodb::bson::doc;
use mongodb::Database;
use sigbot_types::modules::order::events::SigbotTradeEvent;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MongoWalletUpdater {
    database: Arc<Mutex<Option<Arc<Database>>>>,
}

impl MongoWalletUpdater {
    pub async fn new(config: &MongoAppDBProperties) -> Result<Self, Error> {
        let repo = MongoRepository::<()>::new(config)
            .await
            .context("Failed to initialize database connection for wallet trade handler")?;
        let database = repo.get_database();
        Ok(Self {
            database: Arc::new(Mutex::new(Some(Arc::new(database.clone())))),
        })
    }

    async fn get_database(&self) -> Result<Arc<Database>, Error> {
        let db_guard = self.database.lock().await;
        db_guard
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::msg("Database not initialized"))
    }
}

#[async_trait]
impl IWalletUpdater for MongoWalletUpdater {
    async fn upsert(&self, event: &SigbotTradeEvent) -> Result<(), Error> {
        let database = self.get_database().await?;

        // Insert trade record (append-only, immutable ledger)
        // Use check-then-insert for idempotency (handles duplicate messages from EMQX)
        let ledger_collection = database.collection::<mongodb::bson::Document>("s_ledger");
        let filter = doc! {
            "wallet_id": event.wallet_id,
            "order_id": event.order_id,
            "trade_id": event.trade_id
        };
        let existing = ledger_collection.find_one(filter.clone()).await?;

        if existing.is_none() {
            let ledger_doc = doc! {
                "wallet_id": event.wallet_id,
                "order_id": event.order_id,
                "trade_id": event.trade_id,
                "symbol": &event.symbol,
                "side": &event.side,
                "price": event.price,
                "qty": event.qty,
                "fee": event.fee,
                "fee_asset": event.fee_asset.as_ref(),
                "exchange_order_id": event.exchange_order_id.as_ref(),
                "exchange_trade_id": event.exchange_trade_id.as_ref(),
                "ts": mongodb::bson::DateTime::from_millis(event.ts),
            };
            ledger_collection
                .insert_one(ledger_doc)
                .await
                .context("Failed to insert trade record")?;
        }

        info!(
            "Trade record written successfully: wallet_id={}, order_id={}, trade_id={}",
            event.wallet_id, event.order_id, event.trade_id
        );

        // Async trigger derived state update (non-blocking)
        let database_clone = database.clone();
        let event_clone = event.clone();
        tokio::spawn(async move {
            if let Err(e) = MongoWalletUpdater::update_derived_states(&database_clone, &event_clone).await {
                error!("Failed to update derived states: {}", e);
            }
        });

        Ok(())
    }
}

impl MongoWalletUpdater {
    /// Update derived states (balances, positions, equity snapshot)
    async fn update_derived_states(database: &Database, event: &SigbotTradeEvent) -> Result<(), Error> {
        info!("Updating derived states for trade event: {}", event.idempotency_key());

        // Update balances
        MongoWalletUpdater::update_balances(database, event).await?;

        // Update positions
        MongoWalletUpdater::update_positions(database, event).await?;

        // Generate equity snapshot
        MongoWalletUpdater::generate_equity_snapshot(database, event).await?;

        Ok(())
    }

    /// Update balances (derived from ledger table)
    async fn update_balances(database: &Database, event: &SigbotTradeEvent) -> Result<(), Error> {
        let (base_asset, quote_asset) = Self::parse_symbol(&event.symbol)?;
        let balance_collection = database.collection::<mongodb::bson::Document>("s_balances");

        if event.side == "BUY" {
            // Update quote asset balance (spent)
            let quote_filter = doc! { "wallet_id": event.wallet_id, "asset": &quote_asset };
            let fee_amount = if event.fee_asset.as_ref().unwrap_or(&quote_asset) == &quote_asset {
                event.fee
            } else {
                0.0
            };
            let quote_update = doc! {
                "$inc": {
                    "available": -(event.price * event.qty + fee_amount)
                },
                "$set": {
                    "updated_at": mongodb::bson::DateTime::now()
                },
                "$setOnInsert": {
                    "wallet_id": event.wallet_id,
                    "asset": &quote_asset,
                    "locked": 0.0
                }
            };
            balance_collection.update_one(quote_filter, quote_update).await?;

            // Update base asset balance (received)
            let base_filter = doc! { "wallet_id": event.wallet_id, "asset": &base_asset };
            let base_fee_amount = if event.fee_asset.as_ref().unwrap_or(&quote_asset) == &base_asset {
                event.fee
            } else {
                0.0
            };
            let base_update = doc! {
                "$inc": {
                    "available": event.qty - base_fee_amount
                },
                "$set": {
                    "updated_at": mongodb::bson::DateTime::now()
                },
                "$setOnInsert": {
                    "wallet_id": event.wallet_id,
                    "asset": &base_asset,
                    "locked": 0.0
                }
            };
            balance_collection.update_one(base_filter, base_update).await?;
        } else {
            // Update base asset balance (spent)
            let base_filter = doc! { "wallet_id": event.wallet_id, "asset": &base_asset };
            let base_fee_amount = if event.fee_asset.as_ref().unwrap_or(&base_asset) == &base_asset {
                event.fee
            } else {
                0.0
            };
            let base_update = doc! {
                "$inc": {
                    "available": -(event.qty + base_fee_amount)
                },
                "$set": {
                    "updated_at": mongodb::bson::DateTime::now()
                },
                "$setOnInsert": {
                    "wallet_id": event.wallet_id,
                    "asset": &base_asset,
                    "locked": 0.0
                }
            };
            balance_collection.update_one(base_filter, base_update).await?;

            // Update quote asset balance (received)
            let quote_filter = doc! { "wallet_id": event.wallet_id, "asset": &quote_asset };
            let quote_fee_amount = if event.fee_asset.as_ref().unwrap_or(&base_asset) == &quote_asset {
                event.fee
            } else {
                0.0
            };
            let quote_update = doc! {
                "$inc": {
                    "available": event.price * event.qty - quote_fee_amount
                },
                "$set": {
                    "updated_at": mongodb::bson::DateTime::now()
                },
                "$setOnInsert": {
                    "wallet_id": event.wallet_id,
                    "asset": &quote_asset,
                    "locked": 0.0
                }
            };
            balance_collection.update_one(quote_filter, quote_update).await?;
        }

        Ok(())
    }

    /// Update positions (derived from ledger table)
    async fn update_positions(database: &Database, event: &SigbotTradeEvent) -> Result<(), Error> {
        let position_side = if event.side == "BUY" { "LONG" } else { "SHORT" };
        let ledger_collection = database.collection::<mongodb::bson::Document>("s_ledger");
        let position_collection = database.collection::<mongodb::bson::Document>("s_positions");

        // Aggregate positions from ledger
        let pipeline = vec![
            doc! {
                "$match": {
                    "wallet_id": event.wallet_id,
                    "symbol": &event.symbol
                }
            },
            doc! {
                "$group": {
                    "_id": {
                        "wallet_id": "$wallet_id",
                        "symbol": "$symbol"
                    },
                    "size": {
                        "$sum": {
                            "$cond": [
                                { "$eq": ["$side", "BUY"] },
                                "$qty",
                                { "$multiply": ["$qty", -1] }
                            ]
                        }
                    },
                    "total_cost": {
                        "$sum": {
                            "$cond": [
                                { "$eq": ["$side", "BUY"] },
                                { "$multiply": ["$price", "$qty"] },
                                0.0
                            ]
                        }
                    },
                    "total_qty": {
                        "$sum": {
                            "$cond": [
                                { "$eq": ["$side", "BUY"] },
                                "$qty",
                                0.0
                            ]
                        }
                    }
                }
            },
        ];

        let mut cursor = ledger_collection.aggregate(pipeline).await?;

        let mut position_doc = doc! {
            "wallet_id": event.wallet_id,
            "symbol": &event.symbol,
            "side": position_side,
            "realized_pnl": 0.0,
            "updated_at": mongodb::bson::DateTime::now()
        };

        if let Some(result) = cursor.try_next().await? {
            let size = result.get_f64("size").unwrap_or(0.0);
            let total_cost = result.get_f64("total_cost").unwrap_or(0.0);
            let total_qty = result.get_f64("total_qty").unwrap_or(0.0);
            let entry_price = if total_qty > 0.0 { total_cost / total_qty } else { 0.0 };

            position_doc.insert("size", size);
            position_doc.insert("entry_price", entry_price);
        } else {
            position_doc.insert("size", 0.0);
            position_doc.insert("entry_price", 0.0);
        }

        // Upsert position
        let filter = doc! {
            "wallet_id": event.wallet_id,
            "symbol": &event.symbol,
            "side": position_side
        };
        position_collection
            .update_one(filter, doc! { "$set": position_doc })
            .await?;

        Ok(())
    }

    /// Generate equity snapshot
    async fn generate_equity_snapshot(database: &Database, event: &SigbotTradeEvent) -> Result<(), Error> {
        let balance_collection = database.collection::<mongodb::bson::Document>("s_balances");
        let snapshot_collection = database.collection::<mongodb::bson::Document>("equity_snapshots");

        // Aggregate total equity
        let pipeline = vec![
            doc! {
                "$match": {
                    "wallet_id": event.wallet_id
                }
            },
            doc! {
                "$group": {
                    "_id": null,
                    "equity": {
                        "$sum": {
                            "$add": ["$available", "$locked"]
                        }
                    }
                }
            },
        ];

        let mut cursor = balance_collection.aggregate(pipeline).await?;
        let equity = if let Some(result) = cursor.try_next().await? {
            result.get_f64("equity").unwrap_or(0.0)
        } else {
            0.0
        };

        // Insert snapshot
        let snapshot_doc = doc! {
            "wallet_id": event.wallet_id,
            "equity": equity,
            "ts": mongodb::bson::DateTime::now()
        };
        snapshot_collection.insert_one(snapshot_doc).await?;

        Ok(())
    }

    /// 解析交易对，返回基础资产和报价资产
    fn parse_symbol(symbol: &str) -> Result<(String, String), Error> {
        let common_quotes = vec!["USDT", "USDC", "BTC", "ETH", "BNB"];
        for quote in common_quotes {
            if symbol.ends_with(quote) {
                let base = symbol
                    .strip_suffix(quote)
                    .ok_or_else(|| Error::msg(format!("Failed to parse symbol: {}", symbol)))?;
                return Ok((base.to_string(), quote.to_string()));
            }
        }
        Err(Error::msg(format!("Failed to parse symbol: {}", symbol)))
    }
}
