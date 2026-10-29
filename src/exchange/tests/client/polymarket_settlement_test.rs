//! Explicit synthetic RPC receipt fixtures, not evidence of a live trade.
use alloy::{
    primitives::{keccak256, Address, B256, U256},
    rpc::types::{Log, TransactionReceipt},
};
use chrono::{DateTime, Utc};
use polymarket_client_sdk_v2::{contract_config, POLYGON};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sigbot_exchange::client::polymarket_settlement::{PolymarketSettlement, SettlementConfig, SettlementOrder};
use sigbot_types::modules::{
    exchange::instrument::InstrumentSpec,
    messager::{
        envelope::{DurableMessage, MessageKind},
        topic::MessageScope,
    },
    order::{
        execution::ExecutionFact,
        intent::{IntentOrderType, MarketEvidence, OrderSide, TradeIntent},
    },
};

struct Fixture {
    input: DurableMessage<TradeIntent>,
    instrument: InstrumentSpec,
    order: SettlementOrder,
    receipt: Value,
    block_hash: B256,
    at: DateTime<Utc>,
}
impl Fixture {
    fn new(side: OrderSide) -> Self {
        let at = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let exchange = contract_config(POLYGON, false).unwrap().exchange_v2.unwrap();
        let block_hash = B256::repeat_byte(0x33);
        let order_hash = B256::repeat_byte(0x11);
        let maker = Address::repeat_byte(0x22);
        let tx = B256::repeat_byte(0x44);
        let mut maker_topic = [0u8; 32];
        maker_topic[12..].copy_from_slice(maker.as_slice());
        let (maker_cash, taker_cash) = if side == OrderSide::Buy {
            (1_000_000, 3_000_000)
        } else {
            (3_000_000, 1_000_000)
        };
        // ABI words from the pinned official ITrading event, encoded independently
        // of the production generated OrderFilled encoder.
        let words = [
            u64::from(side == OrderSide::Sell),
            555,
            maker_cash,
            taker_cash,
            2500,
            0,
            0,
        ];
        let data = format!(
            "0x{}",
            words.into_iter().map(|word| format!("{word:064x}")).collect::<String>()
        );
        let log = json!({"address":exchange,"topics":[keccak256("OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"),order_hash,B256::from(maker_topic),B256::ZERO],"data":data,"blockNumber":"0x64","blockHash":block_hash,"transactionHash":tx,"transactionIndex":"0x0","logIndex":"0x7","removed":false});
        let receipt = json!({"transactionHash":tx,"transactionIndex":"0x0","blockHash":block_hash,"blockNumber":"0x64","from":maker,"to":exchange,"cumulativeGasUsed":"0x5208","gasUsed":"0x5208","effectiveGasPrice":"0x1","contractAddress":null,"logs":[log],"logsBloom":format!("0x{}","00".repeat(256)),"status":"0x1","type":"0x2"});
        let input = DurableMessage {
            version: 2,
            scope: MessageScope {
                env: "fixture".into(),
                tenant_id: 1,
                space_id: 1,
            },
            workflow_id: Some(1),
            account_id: Some(1),
            node_id: 2,
            target_node_id: None,
            run_id: "receipt-fixture".into(),
            seq: 0,
            attempt_no: 0,
            kind: MessageKind::Event,
            event_time: at,
            available_time: at,
            produced_at: at,
            traceparent: "00-11111111111111111111111111111111-2222222222222222-01".into(),
            payload: TradeIntent {
                account_id: 1,
                wallet_id: 1,
                exchange: "polymarket".into(),
                symbol: "555".into(),
                side,
                quantity: Decimal::from(3),
                order_type: IntentOrderType::Limit,
                limit_price: Some(Decimal::ONE),
                reduce_only: false,
                market: MarketEvidence {
                    source: "polymarket".into(),
                    price: Decimal::ONE,
                    event_time: at,
                    available_time: at,
                },
                execution_group: None,
            },
        };
        let instrument = InstrumentSpec {
            exchange: "polymarket".into(),
            symbol: "555".into(),
            base_asset: "outcome:555".into(),
            quote_asset: "pUSD".into(),
            quantity_step: Decimal::new(1, 6),
            price_tick: Decimal::new(1, 2),
            min_notional: Decimal::ZERO,
        };
        let order = SettlementOrder {
            exchange,
            order_hash,
            maker,
            token_id: U256::from(555),
            side,
            first_block: 90,
        };
        Self {
            input,
            instrument,
            order,
            receipt,
            block_hash,
            at,
        }
    }
    fn decode(&self, finalized: u64) -> anyhow::Result<ExecutionFact> {
        let receipt: TransactionReceipt = serde_json::from_value(self.receipt.clone()).unwrap();
        let log: Log = serde_json::from_value(self.receipt["logs"][0].clone()).unwrap();
        PolymarketSettlement::decode(
            &self.input,
            &self.order,
            &self.instrument,
            &log,
            &receipt,
            self.block_hash,
            finalized,
            self.at,
        )
    }
}

#[test]
fn actual_cash_and_fee_survive_nonterminating_unit_price_for_buy_and_sell() {
    for side in [OrderSide::Buy, OrderSide::Sell] {
        let fixture = Fixture::new(side);
        let fact = fixture.decode(120).unwrap();
        assert_eq!(fact.quantity, Decimal::from(3));
        assert_eq!(fact.quote_quantity, Some(Decimal::ONE));
        assert_ne!(fact.price * fact.quantity, Decimal::ONE);
        assert_eq!(fact.fee, Decimal::new(25, 4));
        assert_eq!(fact.fee_asset, "pUSD");
        assert_eq!(fact.side, side);
        assert_eq!(fact.exchange_trade_id, format!("{:#x}:7", B256::repeat_byte(0x44)));
        assert!(!fact.order_complete);
    }
}

#[test]
fn reverted_unfinalized_reorganized_removed_and_wrong_owner_receipts_are_rejected() {
    assert!(Fixture::new(OrderSide::Buy).decode(99).is_err());
    for mismatch in [
        "status", "removed", "block", "hash", "maker", "token", "exchange", "side",
    ] {
        let mut fixture = Fixture::new(OrderSide::Buy);
        match mismatch {
            "status" => fixture.receipt["status"] = json!("0x0"),
            "removed" => fixture.receipt["logs"][0]["removed"] = json!(true),
            "block" => fixture.block_hash = B256::ZERO,
            "hash" => fixture.order.order_hash = B256::ZERO,
            "maker" => fixture.order.maker = Address::ZERO,
            "token" => fixture.order.token_id = U256::from(556),
            "exchange" => fixture.order.exchange = Address::ZERO,
            _ => fixture.order.side = OrderSide::Sell,
        }
        assert!(fixture.decode(120).is_err(), "accepted {mismatch} mismatch");
    }
}

#[test]
fn missing_rpc_or_unsafe_finality_configuration_prevents_live_observer_construction() {
    assert!(PolymarketSettlement::new(&SettlementConfig {
        rpc_url_env: "SIGBOT_MISSING_SETTLEMENT_RPC_FIXTURE".into(),
        min_confirmations: 64
    })
    .is_err());
    assert!(PolymarketSettlement::new(&SettlementConfig {
        rpc_url_env: "SIGBOT_MISSING_SETTLEMENT_RPC_FIXTURE".into(),
        min_confirmations: 0
    })
    .is_err());
}
#[derive(Clone)]
struct RpcFixture {
    receipt: Value,
    seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    chain: std::sync::Arc<std::sync::atomic::AtomicU64>,
}
impl RpcFixture {
    fn block(&self, number: u64) -> Value {
        json!({"hash":self.receipt["blockHash"],"parentHash":B256::ZERO,"sha3Uncles":B256::ZERO,
            "miner":Address::ZERO,"stateRoot":B256::ZERO,"transactionsRoot":B256::ZERO,
            "receiptsRoot":B256::ZERO,"logsBloom":format!("0x{}","00".repeat(256)),
            "difficulty":"0x0","number":format!("0x{number:x}"),"gasLimit":"0x1000000",
            "gasUsed":"0x5208","timestamp":"0x6553f100","extraData":"0x","mixHash":B256::ZERO,
            "nonce":"0x0000000000000000","baseFeePerGas":"0x1","transactions":[],"uncles":[]})
    }
    async fn reply(
        axum::extract::State(state): axum::extract::State<Self>,
        axum::Json(request): axum::Json<Value>,
    ) -> axum::Json<Value> {
        let method = request["method"].as_str().unwrap();
        state.seen.lock().unwrap().push(method.into());
        let result = match method {
            "eth_chainId" => json!(format!("0x{:x}", state.chain.load(std::sync::atomic::Ordering::SeqCst))),
            "eth_blockNumber" => json!("0xc8"),
            "eth_getCode" => json!("0x6000"),
            "eth_call" => json!(format!("0x{:064x}", 6)),
            "eth_getBlockByNumber" => {
                let value = request["params"][0].as_str().unwrap();
                let number = if value == "finalized" {
                    150
                } else {
                    u64::from_str_radix(value.trim_start_matches("0x"), 16).unwrap()
                };
                state.block(number)
            }
            "eth_getTransactionReceipt" => state.receipt.clone(),
            "eth_getLogs" => {
                let filter = &request["params"][0];
                assert_eq!(filter["topics"][1], state.receipt["logs"][0]["topics"][1]);
                assert_eq!(filter["topics"][0], state.receipt["logs"][0]["topics"][0]);
                let from =
                    u64::from_str_radix(filter["fromBlock"].as_str().unwrap().trim_start_matches("0x"), 16).unwrap();
                let to = u64::from_str_radix(filter["toBlock"].as_str().unwrap().trim_start_matches("0x"), 16).unwrap();
                if from <= 100 && to >= 100 {
                    state.receipt["logs"].clone()
                } else {
                    json!([])
                }
            }
            _ => panic!("unexpected or mutating RPC method: {method}"),
        };
        axum::Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
    }
}

#[tokio::test]
async fn real_alloy_http_reads_finalized_receipts_and_pages_without_resubmitting() {
    let mut fixture = Fixture::new(OrderSide::Buy);
    fixture.input.payload.quantity = Decimal::from(6);
    let mut second = fixture.receipt["logs"][0].clone();
    second["logIndex"] = json!("0x8");
    fixture.receipt["logs"].as_array_mut().unwrap().push(second);
    let state = RpcFixture {
        receipt: fixture.receipt.clone(),
        seen: Default::default(),
        chain: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(POLYGON)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let environment = format!("SIGBOT_POLY_RPC_FIXTURE_{}", address.port());
    std::env::set_var(&environment, format!("http://{address}"));
    let router = axum::Router::new()
        .route("/", axum::routing::post(RpcFixture::reply))
        .with_state(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let reader = PolymarketSettlement::new(&SettlementConfig {
        rpc_url_env: environment.clone(),
        min_confirmations: 64,
    })
    .unwrap();
    assert_eq!(reader.preflight(fixture.order.exchange).await.unwrap(), 200);
    let first = reader
        .read(&fixture.input, &fixture.order, &fixture.instrument, None, 1)
        .await
        .unwrap();
    assert_eq!(first.facts.len(), 1);
    assert!(first.has_more);
    assert_eq!(first.next_cursor.as_deref(), Some("100:8"));
    let repeated = reader
        .read(&fixture.input, &fixture.order, &fixture.instrument, None, 1)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&first.facts).unwrap(),
        serde_json::to_value(&repeated.facts).unwrap()
    );
    let second = reader
        .read(
            &fixture.input,
            &fixture.order,
            &fixture.instrument,
            first.next_cursor.as_deref(),
            1,
        )
        .await
        .unwrap();
    assert_eq!(second.facts.len(), 1);
    assert!(!second.has_more);
    assert_eq!(second.next_cursor.as_deref(), Some("138:0"));
    assert_ne!(first.facts[0].exchange_trade_id, second.facts[0].exchange_trade_id);
    let empty = reader
        .read(
            &fixture.input,
            &fixture.order,
            &fixture.instrument,
            second.next_cursor.as_deref(),
            1,
        )
        .await
        .unwrap();
    assert!(empty.facts.is_empty());
    assert_eq!(empty.next_cursor, second.next_cursor);
    state.chain.store(1, std::sync::atomic::Ordering::SeqCst);
    assert!(reader.preflight(fixture.order.exchange).await.is_err());
    assert!(reader
        .read(&fixture.input, &fixture.order, &fixture.instrument, None, 1)
        .await
        .is_err());
    assert!(!state
        .seen
        .lock()
        .unwrap()
        .iter()
        .any(|method| method.starts_with("eth_send")));
    server.abort();
    std::env::remove_var(environment);
}
