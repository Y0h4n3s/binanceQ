use crate::types::{Order, Side, Symbol, Trade};
use rust_decimal::Decimal;
use std::ops::Mul;

#[derive(Hash, Eq, Ord, PartialOrd, PartialEq, Clone, Debug)]
pub struct Position {
    pub side: Side,
    pub symbol: Symbol,
    pub qty: Decimal,
    pub quote_qty: Decimal,
    pub avg_price: Decimal,
    pub trade_id: u64,
}
impl Position {
    pub fn new(side: Side, symbol: Symbol, qty: Decimal, quote_qty: Decimal) -> Self {
        Self {
            side,
            symbol,
            qty,
            quote_qty,
            avg_price: Decimal::ZERO,
            trade_id: 0,
        }
    }

    pub fn is_long(&self) -> bool {
        self.side == Side::Bid
    }
    pub fn is_short(&self) -> bool {
        self.side == Side::Ask
    }
    pub fn is_open(&self) -> bool {
        self.qty > Decimal::ZERO
    }
    pub fn increment_trade_id(&mut self) -> u64 {
        self.trade_id += 1;
        self.trade_id
    }
    pub fn apply_order(&mut self, order: &Order, time: u64) -> Option<Trade> {
        println!(
            "[?] Delta Order: {} {:?} {:?}",
            order.symbol.symbol, order.order_type, order.side
        );
        println!("[?] Pre-position: {:?}", self);
        if !self.is_open() {
            self.qty = order.quantity;
            self.quote_qty = order.quantity * order.price;
            self.side = order.side.clone();
            self.avg_price = order.price;
            None
        } else {
            let prev_avg_price = self.avg_price;
            self.avg_price = (self.avg_price.mul(self.qty) + order.price.mul(order.quantity))
                / (self.qty + order.quantity);
            match order.side {
                Side::Ask => {
                    if self.side == Side::Ask {
                        self.qty += order.quantity;
                        self.quote_qty += order.quantity * order.price;
                        None
                    } else {
                        let trade = Trade {
                            id: self.increment_trade_id(),
                            order_id: order.id,
                            symbol: order.symbol.clone(),
                            maker: false,
                            price: order.price,
                            commission: Decimal::ZERO,
                            position_side: Side::Bid,
                            side: Side::Ask,
                            realized_pnl: order.quantity * order.price
                                - (prev_avg_price * order.quantity),
                            qty: order.quantity,
                            quote_qty: order.quantity * order.price,
                            time,
                            exit_order_type: order.order_type.clone(),
                        };
                        if self.qty >= order.quantity {
                            self.qty -= order.quantity;
                            self.quote_qty -= order.quantity * order.price;
                        } else {
                            self.qty = order.quantity - self.qty;
                            self.quote_qty = order.quantity * order.price - self.quote_qty;
                            self.side = Side::Ask;
                        }
                        Some(trade)
                    }
                }
                Side::Bid => {
                    if self.side == Side::Bid {
                        self.qty += order.quantity;
                        self.quote_qty += order.quantity * order.price;
                        None
                    } else {
                        let trade = Trade {
                            id: self.increment_trade_id(),
                            order_id: order.id,
                            symbol: order.symbol.clone(),
                            maker: false,
                            price: order.price,
                            commission: Decimal::ZERO,
                            position_side: Side::Ask,
                            side: Side::Bid,
                            realized_pnl: (prev_avg_price * order.quantity)
                                - order.quantity * order.price,
                            qty: order.quantity,
                            quote_qty: order.quantity * order.price,
                            time,
                            exit_order_type: order.order_type.clone(),
                        };
                        if self.qty > order.quantity {
                            self.qty -= order.quantity;
                            self.quote_qty -= order.quantity * order.price;
                        } else {
                            self.qty = order.quantity - self.qty;
                            self.quote_qty = order.quantity * order.price - self.quote_qty;
                            self.side = Side::Bid;
                        }
                        Some(trade)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Position;
    use crate::types::{ClosePolicy, ExchangeId, Order, OrderType, Side, Symbol};
    use rust_decimal::Decimal;
    use uuid::Uuid;
    #[tokio::test]
    async fn test_add_long() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
        let order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(1, 0),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(200, 1),
            quantity: Decimal::new(2, 0),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let no_trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(no_trade_1.is_none());
        assert!(position.is_long());
        assert_eq!(position.qty, Decimal::new(3, 0));

        Ok(())
    }

    #[tokio::test]
    async fn test_add_short() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
        let order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(1, 0),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(200, 1),
            quantity: Decimal::new(2, 0),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let no_trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(no_trade_1.is_none());
        assert!(position.is_short());
        assert_eq!(position.qty, Decimal::new(3, 0));

        Ok(())
    }

    #[tokio::test]
    async fn test_close_long_with_profit() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
        let order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(1, 0),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(200, 1),
            quantity: Decimal::new(1, 0),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let no_trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(no_trade_1.is_some());
        assert!(!position.is_open());
        assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(10, 0));

        Ok(())
    }
    #[tokio::test]
    async fn test_close_long_with_loss() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
        let order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(200, 1),
            quantity: Decimal::new(1, 0),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(1, 0),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let no_trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(no_trade_1.is_some());
        assert!(!position.is_open());
        assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(-10, 0));

        Ok(())
    }

    #[tokio::test]
    async fn test_close_short_with_profit() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
        let order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(1, 0),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(200, 1),
            quantity: Decimal::new(1, 0),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let no_trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(no_trade_1.is_some());
        assert!(!position.is_open());
        assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(10, 0));

        Ok(())
    }

    #[tokio::test]
    async fn test_close_short_with_loss() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
        let order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(200, 1),
            quantity: Decimal::new(1, 0),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(1, 0),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let no_trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(no_trade_1.is_some());
        assert!(!position.is_open());
        assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(-10, 0));

        Ok(())
    }
    #[tokio::test]
    async fn test_partial_close_long_with_profit() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
        let order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(2, 0),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(200, 1),
            quantity: Decimal::new(1, 0),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let no_trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(no_trade_1.is_some());
        assert!(position.is_open());
        assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(10, 0));

        Ok(())
    }
    #[tokio::test]
    async fn test_partial_close_long_with_loss() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
        let order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(200, 1),
            quantity: Decimal::new(2, 0),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(1, 0),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let no_trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(no_trade_1.is_some());
        assert!(position.is_open());
        assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(-10, 0));

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_close_short_with_profit() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
        let order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(2, 0),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(200, 1),
            quantity: Decimal::new(1, 0),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let no_trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(no_trade_1.is_some());
        assert!(position.is_open());
        assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(10, 0));

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_close_short_with_loss() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
        let order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(200, 1),
            quantity: Decimal::new(2, 0),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(1, 0),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let no_trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(no_trade_1.is_some());
        assert!(position.is_open());
        assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(-10, 0));

        Ok(())
    }
}
