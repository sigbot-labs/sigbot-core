use super::*;

use sigbot_types::exchange::models::trade_market::KlineModel;
use sigbot_types::exchange::models::trade_position::OrderType;
use sigbot_types::exchange::models::trade_position::TradeSide;
use sigbot_types::messager::messager::MessagerConfig;
use sigbot_types::strategy::models::strategy_execution::StrategyContext;
use sigbot_types::strategy::strategy::StrategyInfo;
use std::thread;

const TEST_USER_CODE: &str = r#"
import time
from sigbotlib import indicators, risk, signals, utils, data
from sigbotlib import TradingSignal, PyTradeSide, PyOrderType, PyEntryPosition, PyExitPosition

btcusdc_cfg_dict = {}
btcusdc_3m_kline_series = {}
truthsocial_trump_dict = {}

def init():
    global btcusdc_cfg_dict, btcusdc_3m_kline_series, truthsocial_trump_dict
    btcusdc_cfg_dict["series_limit"] = 3000
    btcusdc_3m_kline_series["BTCUSDC::3m"] = []
    print(f"----- [DEBUG] Strategy initialized. -----")
    print(f"btcusdc_cfg_dict: {btcusdc_cfg_dict}")
    print(f"btcusdc_3m_kline_series: {btcusdc_3m_kline_series}")
    print(f"truthsocial_trump_dict: {truthsocial_trump_dict}")

def on_process(context):
    global btcusdc_cfg_dict, btcusdc_3m_kline_series, truthsocial_trump_dict
    batch_klines = context.get("kline_data", {}).get("BTCUSDC::3m")
    print(f"[DEBUG] Extracted latest klines series: {batch_klines}")

    if batch_klines:
        btcusdc_3m_kline_series["BTCUSDC::3m"].extend(batch_klines)
        print(f"----- [DEBUG] Updated cached klines series: {len(btcusdc_3m_kline_series['BTCUSDC::3m'])} -----")
        print(f"btcusdc_3m_kline_series: {btcusdc_3m_kline_series}")

        market_data = context.get("market_data", {})
    if market_data:
        trump_data = market_data.get("truthsocial::posts::trump", {})
        for key, value in trump_data.items():
            truthsocial_trump_dict[key] = value

    print(f"----- [DEBUG] Calculating cost time ... -----")
    time.sleep(0.01)
    print(f"----- [DEBUG] Calculated cost time ... done -----")

    return TradingSignal.long(
        symbol="BTCUSDC",
        quantity=0.1,
        price=99000.0,
        stop_loss_price=98000.0,
        stop_profit_price=105000.0,
        time=0,
        description="Mock Long signal"
    )
    "#;

const TEST_KLINE_1: KlineModel = KlineModel {
    open_price: 100000.0,
    high_price: 105000.0,
    low_price: 99000.0,
    close_price: 102000.0,
    volume: 100000.0,
    open_time: 1717000000000,
    close_time: 1717000000000,
};

const TEST_KLINE_2: KlineModel = KlineModel {
    open_price: 101000.0,
    high_price: 105000.0,
    low_price: 99000.0,
    close_price: 103000.0,
    volume: 100000.0,
    open_time: 1717000000001,
    close_time: 1717000000001,
};

fn build_test_context() -> StrategyContext {
    StrategyContext {
            kline_data: Some( HashMap::from([("BTCUSDC::3m".to_string(), vec![TEST_KLINE_1, TEST_KLINE_2])])),
            market_data: Some( HashMap::from([("truthsocial::posts::trump".to_string(), HashMap::from([("2025-09-29T13:04:21.071Z".to_string(), "In order to make North Carolina, which has completely lost its furniture business to China, and other Countries, GREAT again, I will be imposing substantial Tariffs on any Country that does not make its furniture in the United States. Details to follow!!! President DJT".to_string())]))])),
        }
}

#[test]
fn test_streaming_executor() {
    let executor = StreamingStrategyExecutor::new(
        Arc::new(StrategyArgument {
            properties: None,
            sys_environment: Some(HashMap::new()),
            run_mode: "STREAMING".to_string(),
            messager_config: Arc::new(MessagerConfig::default()),
        }),
        Arc::new(StrategyInfo::default()),
    );

    let result = executor
        .process(&StrategyExecutionInput {
            code: TEST_USER_CODE.to_string(),
            context: build_test_context(),
            run_mode: "STREAMING".to_string(),
        })
        .expect("Failed to process strategy");

    eprintln!("Strategy execution result success: {:?}", result.0.success);
    eprintln!("Strategy execution result result: {:?}", result.0.result);
    eprintln!("Strategy execution result error: {:?}", result.0.error);
    eprintln!("Strategy execution result duration: {:?}ms", result.0.duration_ms);

    assert!(
        result.0.success,
        "Strategy execution should succeed. Error: {:?}",
        result.0.error
    );
    assert!(result.1.is_some());
    let entry_position = result.1.as_ref().unwrap();
    assert!(entry_position.enter_pos.symbol == "BTCUSDC");
    assert!(entry_position.enter_pos.side == TradeSide::LONG);
    assert!(entry_position.enter_pos.order_type == OrderType::LIMITED);
    assert!(entry_position.enter_pos.quantity == 0.1);
    assert!(entry_position.enter_pos.price.is_some());
    assert!(entry_position.enter_pos.price.unwrap() == 99000.0);
    assert!(entry_position.exit_loss.as_ref().unwrap().symbol == "BTCUSDC");
    assert!(entry_position.exit_loss.as_ref().unwrap().quantity_percent == 1.0);
    assert!(entry_position.exit_loss.as_ref().unwrap().price.is_some());
    assert!(entry_position.exit_loss.as_ref().unwrap().price.unwrap() == 98000.0);
    assert!(entry_position.exit_profit.as_ref().unwrap().symbol == "BTCUSDC");
    assert!(entry_position.exit_profit.as_ref().unwrap().quantity_percent == 1.0);
    assert!(entry_position.exit_profit.as_ref().unwrap().price.is_some());
    assert!(entry_position.exit_profit.as_ref().unwrap().price.unwrap() == 105000.0);
    assert!(entry_position.description == "Mock Long signal");
    assert!(entry_position.exit_loss.is_some());
    assert!(entry_position.exit_profit.is_some());
}

#[test]
fn test_streaming_executor_multi_thread_and_mock_cost() {
    let executor = Arc::new(StreamingStrategyExecutor::new(
        Arc::new(StrategyArgument {
            properties: None,
            sys_environment: Some(HashMap::new()),
            run_mode: "STREAMING".to_string(),
            messager_config: Arc::new(MessagerConfig::default()),
        }),
        Arc::new(StrategyInfo::default()),
    ));

    let threads = (0..10).map(|i| {
        let executor0 = executor.clone();
        thread::spawn(move || {
            println!("[DEBUG] Thread {} starting...", i);
            let result = executor0
                .process(&StrategyExecutionInput {
                    code: TEST_USER_CODE.to_string(),
                    context: build_test_context(),
                    run_mode: "STREAMING".to_string(),
                })
                .expect("Failed to process strategy");
            println!("[DEBUG] Thread {} done. - result: {:?}\n", i, result);
        })
    });
    for thread in threads {
        thread.join().unwrap();
    }
    println!("[DEBUG] All threads done.");
}
