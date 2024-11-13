use std::collections::HashMap;
use crate::executors::ExchangeAccountInfo;
use crate::managers::strategy_manager::SignalGenerator;
use crate::types::{ClosePolicy, Kline, Order, OrderType, Side, Trade};
use async_trait::async_trait;
use log::debug;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal_macros::dec;
use std::sync::Arc;
use chrono::Utc;
use rust_decimal::Decimal;
use ta::indicators::{AverageTrueRange, BollingerBands, ExponentialMovingAverage, SimpleMovingAverage};
use ta::{indicators::RelativeStrengthIndex, DataItem, Next};
use tracing::info;

#[derive(Clone, Debug)]
pub struct BreakoutAccumulationStrategy {
    // Technical indicators
    rsi: RelativeStrengthIndex,
    bollinger: BollingerBands,
    ema_short: ExponentialMovingAverage,
    ema_long: ExponentialMovingAverage,
    atr: AverageTrueRange,

    // Support and resistance levels
    support_level: Decimal,
    resistance_level: Decimal,
    recent_lows: Vec<Decimal>,
    recent_highs: Vec<Decimal>,
    levels_period: usize,

    // Risk management parameters
    max_risk_per_trade: Decimal, // e.g., 0.01 for 1%

    // Strategy state
    in_expected_breakout: bool,
    entries_made: usize,
    max_entries: usize,
    last_entry_time: Option<u64>, // Using u64 to match `Kline`'s `close_time`
    entry_cooldown: usize, // Number of candles to wait before next entry
    candles_since_last_entry: usize,

    // Swing low detection
    swing_low_buffer: Vec<Decimal>, // Stores recent lows
    swing_low_period: usize, // Number of candles to consider for swing low

    // Historical data for indicators
    previous_volume: Decimal,
    historical_volatility: Decimal,

}

impl BreakoutAccumulationStrategy {
    pub fn new(rsi_period: usize) -> Self {
        Self {
            // Initialize technical indicators
            rsi: RelativeStrengthIndex::new(14).unwrap(),
            bollinger: BollingerBands::new(20, 2.0).unwrap(),
            ema_short: ExponentialMovingAverage::new(9).unwrap(),
            ema_long: ExponentialMovingAverage::new(21).unwrap(),
            atr: AverageTrueRange::new(14).unwrap(),

            // Initialize support and resistance levels
            support_level: Decimal::new(i64::MAX, 0), // Equivalent to f64::MAX
            resistance_level: Decimal::new(i64::MIN, 0), // Equivalent to f64::MIN
            recent_lows: Vec::new(),
            recent_highs: Vec::new(),
            levels_period: 20, // Adjust as needed

            // Risk management parameters
            max_risk_per_trade: dec!(0.01), // Risk 1% of account per trade

            // Strategy state
            in_expected_breakout: false,
            entries_made: 0,
            max_entries: 5,
            last_entry_time: None,
            entry_cooldown: 5, // Number of candles to wait before next entry
            candles_since_last_entry: 0,

            // Swing low detection
            swing_low_buffer: Vec::new(),
            swing_low_period: 5, // Adjust as needed

            // Historical data for indicators
            previous_volume: Decimal::ZERO,
            historical_volatility: Decimal::ZERO,
        }
    }
    fn update_levels(&mut self, low: Decimal, high: Decimal) {
        self.recent_lows.push(low);
        self.recent_highs.push(high);

        if self.recent_lows.len() > self.levels_period {
            self.recent_lows.remove(0);
        }
        if self.recent_highs.len() > self.levels_period {
            self.recent_highs.remove(0);
        }

        // Update support and resistance levels
        if let Some(&min_low) = self.recent_lows.iter().min() {
            self.support_level = min_low;
        }
        if let Some(&max_high) = self.recent_highs.iter().max() {
            self.resistance_level = max_high;
        }
    }

    fn detect_swing_low(&self) -> bool {
        if self.swing_low_buffer.len() < self.swing_low_period {
            return false;
        }

        let current_low = *self.swing_low_buffer.last().unwrap();
        let previous_lows = &self.swing_low_buffer[..self.swing_low_buffer.len() - 1];

        for &low in previous_lows {
            if current_low >= low {
                return false;
            }
        }

        true
    }
}

#[async_trait]
impl SignalGenerator for BreakoutAccumulationStrategy {
    async fn handle_kline(
        &mut self,
        kline: &Kline,
        account: &Box<Arc<dyn ExchangeAccountInfo>>,
    ) -> Option<Vec<Order>> {
        info!("{:?}", kline);

        // Extract values from kline
        let close = kline.close;
        let low = kline.low;
        let high = kline.high;
        let open = kline.open;
        let volume = kline.volume;

        // Update indicators (convert to f64 for indicator calculations)
        let close_f64 = close.to_f64().unwrap_or(0.0);
        let low_f64 = low.to_f64().unwrap_or(0.0);
        let high_f64 = high.to_f64().unwrap_or(0.0);
        let open_f64 = open.to_f64().unwrap_or(0.0);
        let volume_f64 = volume.to_f64().unwrap_or(0.0);

        let rsi_value = self.rsi.next(close_f64);
        let bollinger_bands = self.bollinger.next(close_f64);
        let ema_short_value = self.ema_short.next(close_f64);
        let ema_long_value = self.ema_long.next(close_f64);
        let di = DataItem::builder()
            .high(high_f64)
            .low(low_f64)
            .close(close_f64)
            .open(open_f64)
            .volume(volume_f64)
            .build().unwrap();
        let atr_value_f64 = self.atr.next(&di);

        // Convert ATR value back to Decimal
        let atr_value = Decimal::from_f64(atr_value_f64).unwrap_or(Decimal::ZERO);

        // Update support and resistance levels
        self.update_levels(low, high);

        // Update swing low buffer
        self.swing_low_buffer.push(low);
        if self.swing_low_buffer.len() > self.swing_low_period {
            self.swing_low_buffer.remove(0);
        }

        // Update candles since last entry
        self.candles_since_last_entry += 1;
        // Part 1: Decide if we expect the market to break up
        let mut conditions_met = 0;
        let min_conditions = 5;

        // Condition 1: Price near support level (within 2%)
        let support_level_with_tolerance = self.support_level * dec!(1.02);
        let price_near_support = close <= support_level_with_tolerance;
        if price_near_support {
            conditions_met += 1;
        }

        // Condition 2: Volume analysis
        if self.previous_volume.is_zero() || volume > self.previous_volume {
            conditions_met += 1;
        }
        self.previous_volume = volume;

        // Condition 3: Volatility contraction
        let bollinger_bandwidth = Decimal::from_f64(bollinger_bands.upper - bollinger_bands.lower).unwrap_or(Decimal::ZERO)
            / Decimal::from_f64(bollinger_bands.average).unwrap_or(Decimal::ONE);

        let volatility_threshold = if self.historical_volatility.is_zero() {
            dec!(0.8)
        } else {
            self.historical_volatility * dec!(0.8)
        };

        if bollinger_bandwidth < volatility_threshold {
            conditions_met += 1;
        }

        // Update historical volatility
        self.historical_volatility = (self.historical_volatility * dec!(0.9)) + (bollinger_bandwidth * dec!(0.1));

        // Condition 4: RSI oversold
        if rsi_value < 30.0 {
            conditions_met += 1;
        }
        // Condition 5: EMA crossover

        if ema_short_value > ema_long_value {
            conditions_met += 1;
        }

        // Check if we expect a breakout
        if conditions_met >= min_conditions {
            if !self.in_expected_breakout {
                // Start a new expected breakout period
                self.in_expected_breakout = true;

            }
        } else {
            // Conditions no longer met, reset expected breakout status
            self.in_expected_breakout = false;
        }

        let mut orders = vec![];

        if self.in_expected_breakout {
            // Part 2: Detect swing low
            let is_swing_low = self.detect_swing_low();

            // Get position and open orders
            let position = account.get_position(&kline.symbol).await;
            let open_orders = account.get_open_orders(&kline.symbol).await;

            let can_enter = self.candles_since_last_entry >= self.entry_cooldown;
            let position_size = dec!(100); // Adjust based on your risk management
            self.entries_made = (position.qty / position_size).to_usize().unwrap();
            let entries_remaining = self.entries_made < self.max_entries;
            // info!("{} {} {} {} {}", is_swing_low, can_enter, entries_remaining, self.entries_made, self.candles_since_last_entry);
            if is_swing_low && can_enter && entries_remaining   {
                // Part 3: Place order
                // Calculate entry, stop-loss, take-profit prices

                // Entry price is the current close price
                let entry_price = close;

                // Stop-loss price (below support level)
                let stop_loss_price = self.support_level - (atr_value * dec!(1.5));

                // Take-profit price
                let tp_multiplier = self.resistance_level +  (atr_value * Decimal::from_usize(self.entries_made).unwrap());
                let tp_price = entry_price + (atr_value * tp_multiplier);


                // Entry Order
                orders.push(Order {
                    id: uuid::Uuid::new_v4(),
                    symbol: kline.symbol.clone(),
                    side: Side::Bid,
                    price: entry_price,
                    quantity: position_size,
                    time: kline.close_time,
                    order_type: OrderType::Limit,
                    lifetime: 0,
                    close_policy: ClosePolicy::None,
                });

                // Stop-Loss Order
                orders.push(Order {
                    id: uuid::Uuid::new_v4(),
                    symbol: kline.symbol.clone(),
                    side: Side::Ask,
                    price: stop_loss_price,
                    quantity: position_size,
                    time: kline.close_time,
                    order_type: OrderType::StopLossLimit,
                    lifetime: 0,
                    close_policy: ClosePolicy::None,
                });

                // Take-Profit Orders
                orders.push(Order {
                    id: uuid::Uuid::new_v4(),
                    symbol: kline.symbol.clone(),
                    side: Side::Ask,
                    price: tp_price,
                    quantity: position_size,
                    time: kline.close_time,
                    order_type: OrderType::TakeProfitLimit,
                    lifetime: 0,
                    close_policy: ClosePolicy::None,
                });

                // Update state
                self.entries_made += 1;
                self.last_entry_time = Some(kline.close_time);
                self.candles_since_last_entry = 0;
            }
        }

        Some(orders).filter(|orders| !orders.is_empty())
    }

    async fn handle_trade(&mut self, trade: &Trade, _account: &Box<Arc<dyn ExchangeAccountInfo>>) -> Option<Vec<Order>> {
        if trade.position_side == Side::Bid {
            self.entries_made += 1;
            self.candles_since_last_entry = 0;
        }
        None
    }
}
