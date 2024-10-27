use std::sync::Arc;
use rust_decimal_macros::dec;
use ta::{indicators::RelativeStrengthIndex, Next};
use ta::indicators::SimpleMovingAverage;
use crate::executors::ExchangeAccountInfo;
use crate::managers::strategy_manager::SignalGenerator;
use crate::types::{ClosePolicy, Kline, Order, OrderType, Side};

#[derive(Clone, Debug)]
pub struct SimpleRSIStrategy {
    rsi: RelativeStrengthIndex,
    sma1: SimpleMovingAverage,
    sma2: SimpleMovingAverage,
    sma3: SimpleMovingAverage,

}

impl SimpleRSIStrategy {
    pub fn new(rsi_period: usize) -> Self {
        let rsi_indicator = RelativeStrengthIndex::new(rsi_period)
            .expect("Failed to construct RSI indicator");

        Self {
            rsi: rsi_indicator,
            sma1: SimpleMovingAverage::new(11).unwrap(),
            sma2: SimpleMovingAverage::new(43).unwrap(),
            sma3: SimpleMovingAverage::new(93).unwrap(),
        }
    }
}

impl SignalGenerator for SimpleRSIStrategy {
    async fn handle_kline(&mut self, kline: &Kline, account: &Box<Arc<dyn ExchangeAccountInfo>>) -> Option<Vec<Order>> {
        let rsi = self.rsi.next(kline.close);


        let position = account.get_position(&kline.symbol).await;


        let s1 = self.sma1.next(kline.close);
        let s2 = self.sma2.next(kline.close);
        let s3 = self.sma3.next(kline.close);
        let mut direction = 0;
        if s1 > s2 && s2 > s3 {
            direction = 1
        } else if s1 < s2 && s2 < s3 {
            direction = -1
        }

        let mut orders = vec![];
        if  direction == -1 {
            if rsi < 30.0 && position.is_open() {
                orders.push(Order {
                    id: Default::default(),
                    symbol: kline.symbol.clone(),
                    side: Side::Ask,
                    price: kline.close,
                    quantity: position.qty,
                    time: 0,
                    order_type: OrderType::Market,
                    lifetime: 0,
                    close_policy: ClosePolicy::None,
                })
            }
        }
        if direction == 1{
                if rsi > 70.0 && !position.is_open() {
                    orders.push(Order {
                        id: Default::default(),
                        symbol: kline.symbol.clone(),
                        side: Side::Bid,
                        price: kline.close,
                        quantity: dec!(100),
                        time: 0,
                        order_type: OrderType::Market,
                        lifetime: 0,
                        close_policy: ClosePolicy::None,
                    })
                }

        }

        Some(orders)
    }
}
