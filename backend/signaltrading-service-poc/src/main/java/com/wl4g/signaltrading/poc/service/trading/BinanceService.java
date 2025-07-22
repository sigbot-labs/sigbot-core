package com.wl4g.signaltrading.poc.service.trading;

import com.binance.connector.client.common.ApiException;
import com.binance.connector.client.common.ApiResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.api.DerivativesTradingUsdsFuturesRestApi;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.model.Interval;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.model.KlineCandlestickDataResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.model.NewOrderRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.model.Side;
import com.binance.connector.client.derivatives_trading_usds_futures.rest.model.SymbolPriceTickerV2Response;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * The {@link BinanceService}
 *
 * @author James Wong
 * @version v1.0 Saturday
 * @since v1.0
 */
@Slf4j
@RequiredArgsConstructor
public class BinanceService extends CEXService {

    private final DerivativesTradingUsdsFuturesRestApi futuresApi;

    public ExchangeProvider getProvider() {
        return ExchangeProvider.BINANCE;
    }

    @Override
    public Double getCurrentPrice(String symbol) throws ApiException {
        try {
            // 根据示例代码，直接调用 symbolPriceTickerV2 方法
            ApiResponse<SymbolPriceTickerV2Response> response = futuresApi.symbolPriceTickerV2(symbol);
            log.info("获取价格响应: {}", response.getData());

            SymbolPriceTickerV2Response data = response.getData();
            if (data != null) {
                // 尝试多种方式获取价格字段
                String priceStr = null;

                // 方法1: 尝试使用 getPrice() 方法（如果存在）
                try {
                    java.lang.reflect.Method getPriceMethod = data.getClass().getMethod("getPrice");
                    Object priceObj = getPriceMethod.invoke(data);
                    if (priceObj != null) {
                        priceStr = priceObj.toString();
                    }
                } catch (Exception e) {
                    // 如果 getPrice() 不存在，尝试其他方法
                }

                // 方法2: 尝试使用 price() 方法（如果存在）
                if (priceStr == null) {
                    try {
                        java.lang.reflect.Method priceMethod = data.getClass().getMethod("price");
                        Object priceObj = priceMethod.invoke(data);
                        if (priceObj != null) {
                            priceStr = priceObj.toString();
                        }
                    } catch (Exception e) {
                        // 如果 price() 不存在，继续尝试
                    }
                }

                // 方法3: 如果响应是 Map 类型，直接获取
                if (priceStr == null && data instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> dataMap = (Map<String, Object>) data;
                    Object priceObj = dataMap.get("price");
                    if (priceObj != null) {
                        priceStr = priceObj.toString();
                    }
                }

                if (priceStr != null) {
                    return Double.parseDouble(priceStr);
                }
            }
            return null;
        } catch (Exception e) {
            log.error("获取价格失败: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * <a href="https://github.com/binance/binance-connector-java/blob/master/examples/derivatives-trading-usds-futures/src/main/java/com/binance/connector/client/derivatives_trading_usds_futures/rest/marketdata/KlineCandlestickDataExample.java">Binance KlineCandlestickDataExample.java</a>
     */
    @Override
    public List<List<Object>> getKlines(String symbol, String interval, Integer limit) throws ApiException {
        try {
            // 将字符串 interval 转换为 Interval 枚举
            Interval intervalEnum = Interval.fromValue(interval);
            if (intervalEnum == null) {
                throw new IllegalArgumentException("不支持的 K线间隔: " + interval);
            }

            // 根据示例代码，使用 klineCandlestickData 方法
            // startTime 和 endTime 设为 null 表示获取最近的 K线
            ApiResponse<KlineCandlestickDataResponse> response = futuresApi.klineCandlestickData(
                    symbol,
                    intervalEnum,
                    null,  // startTime
                    null,  // endTime
                    Long.valueOf(limit)
            );

            KlineCandlestickDataResponse data = response.getData();
            if (data != null) {
                // 尝试通过反射获取 K线数据
                // KlineCandlestickDataResponse 可能包含一个 List 字段
                try {
                    // 方法1: 尝试 getData() 方法
                    java.lang.reflect.Method getDataMethod = data.getClass().getMethod("getData");
                    Object klinesData = getDataMethod.invoke(data);
                    if (klinesData instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<List<Object>> klines = (List<List<Object>>) klinesData;
                        return klines;
                    }
                } catch (NoSuchMethodException e) {
                    // 如果 getData() 不存在，尝试其他方法
                    log.debug("getData() 方法不存在，尝试其他方式");
                } catch (Exception e) {
                    log.warn("通过 getData() 获取K线数据失败", e);
                }

                // 方法2: 尝试直接访问字段
                try {
                    java.lang.reflect.Field[] fields = data.getClass().getDeclaredFields();
                    for (java.lang.reflect.Field field : fields) {
                        field.setAccessible(true);
                        Object fieldValue = field.get(data);
                        if (fieldValue instanceof List) {
                            @SuppressWarnings("unchecked")
                            List<List<Object>> klines = (List<List<Object>>) fieldValue;
                            return klines;
                        }
                    }
                } catch (Exception e) {
                    log.warn("通过字段访问获取K线数据失败", e);
                }

                // 方法3: 如果响应是 Map，尝试获取数据
                if (data instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> dataMap = (Map<String, Object>) data;
                    Object klinesObj = dataMap.get("data");
                    if (klinesObj instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<List<Object>> klines = (List<List<Object>>) klinesObj;
                        return klines;
                    }
                }

                // 方法4: 尝试 toString() 然后解析（最后的手段）
                log.warn("无法通过标准方式解析K线数据，响应类型: {}, 内容: {}",
                        data.getClass().getName(), data);
            }

            log.warn("无法解析K线数据响应: {}", data);
            return null;
        } catch (Exception e) {
            log.error("获取K线数据失败: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Long openPosition(TradeSignal signal) throws ApiException {
        try {
            final var request = new NewOrderRequest();
            request.symbol(signal.getOpenPosition().getSymbol());
            request.side(Side.valueOf(signal.getOpenPosition().getSide().name()));
            request.type(signal.getOpenPosition().getType().name());
            request.quantity(formatQuantity(signal.getOpenPosition().getQuantity()));

            log.info("[OPEN_POS] Opening position - symbol={}, side={}, quantity={}",
                    signal.getOpenPosition().getSymbol(), signal.getOpenPosition().getSide(), signal.getOpenPosition().getQuantity());

            final var response = futuresApi.newOrder(request);
            final var orderResponse = response.getData();
            log.info("[OPEN_POS] Opened position - orderId={}, price={}", orderResponse.getOrderId(), orderResponse.getAvgPrice());

            return orderResponse.getOrderId();
        } catch (Exception e) {
            log.error("Failed to opening market position: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Long setStopLoss(Long originalOrderId, TradeSignal.StopPosition position) throws ApiException {
        try {
            final var request = new NewOrderRequest();
            request.symbol(position.getSymbol());
            request.closePosition("true");
            // The stop-loss direction is opposite to the opened direction.
            request.side(Side.valueOf(position.getSide().opposite().name()));
            if (position.getType() == TradeSignal.OrderType.MARKET) {
                request.type("STOP_MARKET");
            } else if (position.getType() == TradeSignal.OrderType.LIMITED) {
                request.type("STOP");
                request.stopPrice(formatPrice(position.getPrice()));
            }
            log.info("[STOP_LOSS] Setting up - symbol={}, side={}, price={}", position.getSymbol(), position.getSide().opposite(), position.getPrice());

            final var response = futuresApi.newOrder(request);
            final var orderResponse = response.getData();
            log.info("[STOP_LOSS] Set success - orderId: {}", orderResponse.getOrderId());

            return orderResponse.getOrderId();
        } catch (Exception e) {
            log.error("[STOP_LOSS] Set failed", e);
            throw e;
        }
    }

    @Override
    public Long setStopProfit(Long originalOrderId, TradeSignal.StopPosition position) throws ApiException {
        try {
            final var request = new NewOrderRequest();
            request.symbol(position.getSymbol());
            request.closePosition("true");
            // The stop-loss direction is opposite to the opened direction.
            request.side(Side.valueOf(position.getSide().opposite().name()));
            if (position.getType() == TradeSignal.OrderType.MARKET) {
                request.type("TAKE_PROFIT_MARKET");
            } else if (position.getType() == TradeSignal.OrderType.LIMITED) {
                request.type("TAKE_PROFIT");
                request.stopPrice(formatPrice(position.getPrice()));
            }
            log.info("[STOP_PROFIT] Setting up - symbol={}, side={}, price={}", position.getSymbol(), position.getSide().opposite(), position.getPrice());

            final var response = futuresApi.newOrder(request);
            final var orderResponse = response.getData();
            log.info("[STOP_PROFIT] Set up success - orderId: {}", orderResponse.getOrderId());

            return orderResponse.getOrderId();
        } catch (Exception e) {
            log.error("[STOP_PROFIT] Set up failed", e);
            throw e;
        }
    }

}
