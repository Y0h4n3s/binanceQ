use std::collections::HashMap;
use crate::executors::ExchangeAccountInfo;
use crate::managers::strategy_manager::SignalGenerator;
use crate::types::{ClosePolicy, Kline, Order, OrderType, Side};
use async_trait::async_trait;
use log::debug;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal_macros::dec;
use std::sync::Arc;
use chrono::Utc;
use rust_decimal::Decimal;
use ta::indicators::{AverageTrueRange, BollingerBands, ExponentialMovingAverage, SimpleMovingAverage};
use ta::{indicators::RelativeStrengthIndex, Next};

#[derive(Clone, Debug)]
pub struct BreakoutAccumulationStrategy {
    // Technical indicators
    rsi: RelativeStrengthIndex,
    bollinger: BollingerBands,
    ema_short: ExponentialMovingAverage,
    ema_long: ExponentialMovingAverage,
    atr: AverageTrueRange,

    // Support and resistance levels
    support_level: f64,
    resistance_level: f64,

    // Risk management parameters
    max_risk_per_trade: f64, // e.g., 0.01 for 1%
}

impl BreakoutAccumulationStrategy {
    pub fn new(rsi_period: usize) -> Self {
        let rsi_indicator =
            RelativeStrengthIndex::new(rsi_period).expect("Failed to construct RSI indicator");

        Self {
            // Initialize technical indicators
            rsi: rsi_indicator,
            bollinger: BollingerBands::new(20, 2.0).unwrap(),
            ema_short: ExponentialMovingAverage::new(9).unwrap(),
            ema_long: ExponentialMovingAverage::new(21).unwrap(),
            atr: AverageTrueRange::new(14).unwrap(),

            // Initialize support and resistance levels
            support_level: f64::MAX,
            resistance_level: f64::MIN,

            // Risk management parameters
            max_risk_per_trade: 0.01, // Risk 1% of account per trade
        }
    }


    fn update_levels(&mut self, low: f64, high: f64) {
        // Update support and resistance levels based on recent candles
        // You may want to use a more sophisticated method in practice
        self.support_level = self.support_level.min(low);
        self.resistance_level = self.resistance_level.max(high);
    }

    // Method to calculate adjusted entry price based on confidence
    fn calculate_entry_price(&self, confidence: f64) -> f64 {
        let range = self.resistance_level - self.support_level;

        let entry_price = if confidence > 0.0 {
            // For upward confidence, higher confidence means higher entry price within the range
            self.support_level + (confidence * range)
        } else if confidence < 0.0 {
            // For downward confidence, higher (negative) confidence means lower entry price within the range
            self.resistance_level + (confidence * range) // confidence is negative
        } else {
            // Neutral confidence
            (self.support_level + self.resistance_level) / 2.0
        };

        entry_price
    }

    // Method to calculate position size based on confidence
    fn calculate_position_size(
        &self,
        account_equity: f64,
        stop_loss_distance: f64,
        confidence: f64,
    ) -> f64 {
        let total_risk_amount = account_equity * self.max_risk_per_trade;
        let base_position_size = total_risk_amount / stop_loss_distance;

        // Adjust position size based on confidence level
        // Ensure confidence is within [0.0, 1.0]
        let adjusted_confidence = confidence.abs().max(0.0).min(1.0);

        let position_size = base_position_size * adjusted_confidence;

        position_size
    }
}

#[async_trait]
impl SignalGenerator for BreakoutAccumulationStrategy {
    async fn handle_kline(
        &mut self,
        kline: &Kline,
        account: &Box<Arc<dyn ExchangeAccountInfo>>,
    ) -> Option<Vec<Order>> {
        let close = kline.close.to_f64().unwrap_or(0.0);
        let low = kline.low.to_f64().unwrap_or(0.0);
        let high = kline.high.to_f64().unwrap_or(0.0);
        let rsi = self.rsi.next(close);

        let position = account.get_position(&kline.symbol).await;
        let open_orders = account.get_open_orders(&kline.symbol).await;


        // Update indicators
        let rsi_value = self.rsi.next(close);
        let bollinger = self.bollinger.next(close);
        let ema_short = self.ema_short.next(close);
        let ema_long = self.ema_long.next(close);
        let atr_value = self.atr.next(close);

        // Update support and resistance levels
        self.update_levels(low, high);

        // Part 1: Calculate confidence level
        let mut confidence: f64 = 0.0;

        // Example logic for determining confidence
        if ema_short > ema_long {
            confidence += 0.1; // Upward trend indicator
        } else if ema_short < ema_long {
            confidence -= 0.1; // Downward trend indicator
        }

        if rsi_value > 60.0 {
            confidence += 0.1; // Bullish momentum
        } else if rsi_value < 40.0 {
            confidence -= 0.1; // Bearish momentum
        }

        if close > bollinger.average {
            confidence += 0.1; // Price above middle Bollinger Band
        } else if close < bollinger.average {
            confidence -= 0.1; // Price below middle Bollinger Band
        }

        if confidence < 0.2 {
            return None
        }
        // Clamp confidence to [-1.0, 1.0]
        confidence = confidence.max(-1.0).min(1.0);

        // Part 2: Calculate adjusted entry price
        let entry_price = self.calculate_entry_price(confidence);

        // Part 3: Calculate position size

        let stop_loss_distance = (self.resistance_level - self.support_level) / 2.0; // Example calculation

        // Determine order side based on confidence sign
        let direction = if confidence > 0.0 {
            1
        } else {
            -1
        };

        // Optional: Calculate stop-loss and take-profit prices
        let stop_loss_price = if confidence > 0.0 {
            // For long positions, stop loss below support level
            self.support_level - (atr_value * 2.5)
        } else {
            // For short positions, stop loss above resistance level
            self.resistance_level + (atr_value * 0.5)
        };
        // Optional: Calculate stop-loss and take-profit prices
        let take_profit_price = if confidence > 0.0 {
            // For long positions, stop loss below support level
            self.resistance_level + (atr_value * 3.5)
        } else {
            // For short positions, stop loss above resistance level
            self.support_level - (atr_value * 0.5)
        };
        let mut orders = vec![];


        if direction == 1 {
            if rsi > 70.0 && !position.is_open() && open_orders.is_empty() {
                orders.push(Order {
                    id: uuid::Uuid::new_v4(),
                    symbol: kline.symbol.clone(),
                    side: Side::Bid,
                    price: Decimal::from_f64(take_profit_price).unwrap(),
                    quantity: dec!(100),
                    time: kline.close_time,
                    order_type: OrderType::TakeProfitLimit,
                    lifetime: 0,
                    close_policy: ClosePolicy::None,
                });
                orders.push(Order {
                    id: uuid::Uuid::new_v4(),
                    symbol: kline.symbol.clone(),
                    side: Side::Bid,
                    price: Decimal::from_f64(stop_loss_price).unwrap(),
                    quantity: dec!(100),
                    time: kline.close_time,
                    order_type: OrderType::StopLossLimit,
                    lifetime: 0,
                    close_policy: ClosePolicy::None,
                });
                orders.push(Order {
                    id: uuid::Uuid::new_v4(),
                    symbol: kline.symbol.clone(),
                    side: Side::Ask,
                    price: Decimal::from_f64(entry_price).unwrap(),
                    quantity: dec!(100),
                    time: kline.close_time,
                    order_type: OrderType::Limit,
                    lifetime: 0,
                    close_policy: ClosePolicy::None,
                });
            }
        }

        Some(orders)
    }

}
