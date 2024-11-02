use crate::types::{Order, Side, Symbol, Trade};
use rust_decimal::Decimal;

#[derive(Hash, Eq, Ord, PartialOrd, PartialEq, Clone, Debug)]
pub struct Position {
    pub side: Side,
    pub symbol: Symbol,
    pub qty: Decimal,
    pub quote_qty: Decimal,
    pub avg_price: Decimal,
    pub trade_id: u64,
}

// TODO: fix average price calculation on side change/position reversal
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
        // println!(
        //     "[?] Delta Order: {} {:?} {:?} {} {}",
        //     order.symbol.symbol, order.order_type, order.side, order.quantity, order.price
        // );
        // println!("[?] Pre-position: {:?}", self);
        if !self.is_open() {
            self.qty = order.quantity;
            self.quote_qty = order.quantity * order.price;
            self.side = order.side.clone();
            self.avg_price = order.price;
            None
        } else {
            let prev_avg_price = self.avg_price;
            match order.side {
                Side::Ask => {
                    if self.side == Side::Ask {
                        self.avg_price = ((self.avg_price * self.qty)
                            + (order.price * order.quantity))
                            / (order.quantity + self.qty);
                        self.qty += order.quantity;
                        self.quote_qty += order.quantity * order.price;
                        None
                    } else {
                        let realized_pnl = if order.quantity >= self.qty {
                            (order.price - prev_avg_price) * self.qty
                        } else {
                            (order.price - prev_avg_price) * order.quantity
                        };
                        let trade = Trade {
                            id: self.increment_trade_id(),
                            order_id: order.id,
                            symbol: order.symbol.clone(),
                            maker: false,
                            price: order.price,
                            commission: Decimal::ZERO,
                            position_side: Side::Bid,
                            side: Side::Ask,
                            realized_pnl,
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
                            self.avg_price = order.price;
                        }
                        Some(trade)
                    }
                }
                Side::Bid => {
                    if self.side == Side::Bid {
                        self.avg_price = ((self.avg_price * self.qty)
                            + (order.price * order.quantity))
                            / (order.quantity + self.qty);

                        self.qty += order.quantity;
                        self.quote_qty += order.quantity * order.price;
                        None
                    } else {
                        let realized_pnl = if order.quantity >= self.qty {
                            (prev_avg_price - order.price) * self.qty
                        } else {
                            (prev_avg_price - order.price) * order.quantity
                        };
                        let trade = Trade {
                            id: self.increment_trade_id(),
                            order_id: order.id,
                            symbol: order.symbol.clone(),
                            maker: false,
                            price: order.price,
                            commission: Decimal::ZERO,
                            position_side: Side::Ask,
                            side: Side::Bid,
                            realized_pnl,
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
                            self.side = Side::Bid;
                            self.avg_price = order.price;
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
    use rust_decimal_macros::dec;
    use uuid::Uuid;
    #[test]
    fn test_add_long() -> anyhow::Result<()> {
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
            price: dec!(10),
            quantity: dec!(1),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: dec!(20),
            quantity: dec!(2),
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
        assert_eq!(position.qty, dec!(3));

        Ok(())
    }

    #[test]
    fn test_add_short() -> anyhow::Result<()> {
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
            price: dec!(10),
            quantity: dec!(1),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: dec!(20),
            quantity: dec!(2),
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
        assert_eq!(position.qty, dec!(3));

        Ok(())
    }

    #[test]
    fn test_close_long_with_profit() -> anyhow::Result<()> {
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
            price: dec!(10),
            quantity: dec!(1),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: dec!(20),
            quantity: dec!(1),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(trade_1.is_some());
        assert!(!position.is_open());
        assert_eq!(trade_1.unwrap().realized_pnl, dec!(10));

        Ok(())
    }
    #[test]
    fn test_close_long_with_loss() -> anyhow::Result<()> {
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
            price: dec!(20),
            quantity: dec!(200),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: dec!(21.2),
            quantity: dec!(200),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(trade_1.is_some());
        assert!(!position.is_open());
        assert_eq!(trade_1.unwrap().realized_pnl, dec!(240));

        Ok(())
    }

    #[test]
    fn test_close_short_with_profit() -> anyhow::Result<()> {
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
            price: dec!(10),
            quantity: dec!(1),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: dec!(5),
            quantity: dec!(1),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(trade_1.is_some());
        assert!(!position.is_open());
        assert_eq!(trade_1.unwrap().realized_pnl, dec!(5));

        Ok(())
    }

    #[test]
    fn test_close_short_with_loss() -> anyhow::Result<()> {
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
            price: dec!(20),
            quantity: dec!(200),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: dec!(21.2),
            quantity: dec!(200),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(trade_1.is_some());
        assert!(!position.is_open());
        assert_eq!(trade_1.unwrap().realized_pnl, dec!(-240));
        Ok(())
    }
    #[test]
    fn test_partial_close_long_with_profit() -> anyhow::Result<()> {
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
        let trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(trade_1.is_some());
        assert!(position.is_open());
        assert_eq!(trade_1.unwrap().realized_pnl, dec!(10));

        Ok(())
    }
    #[test]
    fn test_partial_close_long_with_loss() -> anyhow::Result<()> {
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
            price: dec!(20),
            quantity: dec!(2),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: dec!(10),
            quantity: dec!(1.2),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(trade_1.is_some());
        let trade = trade_1.unwrap();
        assert!(position.is_open());
        assert_eq!(position.qty, dec!(0.8));
        assert_eq!(trade.realized_pnl, Decimal::new(-12, 0));

        Ok(())
    }

    #[test]
    fn test_partial_close_short_with_profit() -> anyhow::Result<()> {
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
            price: dec!(10),
            quantity: dec!(2),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: dec!(5),
            quantity: dec!(1),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(trade_1.is_some());
        assert!(position.is_open());
        assert_eq!(trade_1.unwrap().realized_pnl, dec!(5));

        Ok(())
    }

    #[test]
    fn test_partial_close_short_with_loss() -> anyhow::Result<()> {
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
            price: dec!(20),
            quantity: dec!(2),
            time: 1,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: dec!(30),
            quantity: dec!(1),
            time: 2,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };
        let no_trade = position.apply_order(&order_1, 0);
        let trade_1 = position.apply_order(&order_2, 0);
        assert!(no_trade.is_none());
        assert!(trade_1.is_some());
        assert!(position.is_open());
        assert_eq!(trade_1.unwrap().realized_pnl, dec!(-10));

        Ok(())
    }

    #[test]
    fn test_open_new_position() {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position::new(Side::Bid, symbol.clone(), Decimal::ZERO, Decimal::ZERO);

        let order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: dec!(50000.0),
            quantity: dec!(1.0),
            time: 0,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        let trade = position.apply_order(&order, 0);
        assert!(trade.is_none());
        assert_eq!(position.qty, dec!(1.0));
        assert_eq!(position.avg_price, dec!(50000.0));
        assert_eq!(position.side, Side::Bid);
    }

    #[test]
    fn test_increase_existing_position_same_side() {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position {
            side: Side::Bid,
            symbol: symbol.clone(),
            qty: dec!(1.0),
            quote_qty: dec!(50000.0),
            avg_price: dec!(50000.0),
            trade_id: 0,
        };

        let order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: dec!(55000.0),
            quantity: dec!(1.0),
            time: 0,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        let trade = position.apply_order(&order, 0);
        assert!(trade.is_none());
        assert_eq!(position.qty, dec!(2.0));
        assert_eq!(position.avg_price, dec!(52500.0)); // (50000*1 + 55000*1) / 2
        assert_eq!(position.side, Side::Bid);
    }

    #[test]
    fn test_partially_close_position() {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position {
            side: Side::Bid,
            symbol: symbol.clone(),
            qty: dec!(2.0),
            quote_qty: dec!(100000.0),
            avg_price: dec!(50000.0),
            trade_id: 0,
        };

        let order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: dec!(55000.0),
            quantity: dec!(1.0),
            time: 0,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        let trade = position.apply_order(&order, 0);
        assert!(trade.is_some());
        let trade = trade.unwrap();

        assert_eq!(trade.realized_pnl, dec!(5000.0)); // (55000 - 50000) * 1
        assert_eq!(position.qty, dec!(1.0));
        assert_eq!(position.side, Side::Bid);
        assert_eq!(position.avg_price, dec!(50000.0));
    }

    #[test]
    fn test_completely_close_position() {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position {
            side: Side::Bid,
            symbol: symbol.clone(),
            qty: dec!(1.0),
            quote_qty: dec!(50000.0),
            avg_price: dec!(50000.0),
            trade_id: 0,
        };

        let order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: dec!(55000.0),
            quantity: dec!(1.0),
            time: 0,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        let trade = position.apply_order(&order, 0);
        assert!(trade.is_some());
        let trade = trade.unwrap();

        assert_eq!(trade.realized_pnl, dec!(5000.0)); // (55000 - 50000) * 1
        assert_eq!(position.qty, Decimal::ZERO);
        assert_eq!(position.side, Side::Bid); // Side remains, but qty is zero
    }

    #[test]
    fn test_reverse_position() {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position {
            side: Side::Bid,
            symbol: symbol.clone(),
            qty: dec!(1.0),
            quote_qty: dec!(50000.0),
            avg_price: dec!(50000.0),
            trade_id: 0,
        };

        let order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: dec!(55000.0),
            quantity: dec!(2.0),
            time: 0,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        let trade = position.apply_order(&order, 0);
        assert!(trade.is_some());
        let trade = trade.unwrap();

        assert_eq!(trade.realized_pnl, dec!(5000.0)); // (55000 - 50000) * 1
        assert_eq!(position.qty, dec!(1.0));
        assert_eq!(position.side, Side::Ask);
        assert_eq!(position.avg_price, dec!(55000.0));
    }

    #[test]
    fn test_apply_order_no_existing_position() {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);

        let order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: dec!(50000.0),
            quantity: dec!(1.0),
            time: 0,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        let trade = position.apply_order(&order, 0);
        assert!(trade.is_none());
        assert_eq!(position.qty, dec!(1.0));
        assert_eq!(position.avg_price, dec!(50000.0));
        assert_eq!(position.side, Side::Bid);
    }

    #[test]
    fn test_apply_order_opposite_side_same_qty() {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position {
            side: Side::Ask,
            symbol: symbol.clone(),
            qty: dec!(1.0),
            quote_qty: dec!(50000.0),
            avg_price: dec!(50000.0),
            trade_id: 0,
        };

        let order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: dec!(45000.0),
            quantity: dec!(1.0),
            time: 0,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        let trade = position.apply_order(&order, 0);
        assert!(trade.is_some());
        let trade = trade.unwrap();

        assert_eq!(trade.realized_pnl, dec!(5000.0)); // (50000 - 45000) * 1
        assert_eq!(position.qty, Decimal::ZERO);
        assert_eq!(position.side, Side::Ask);
        assert_eq!(position.avg_price, dec!(50000));
    }

    #[test]
    fn test_apply_order_partial_reverse_position() {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut position = Position {
            side: Side::Ask,
            symbol: symbol.clone(),
            qty: dec!(2.0),
            quote_qty: dec!(100000.0),
            avg_price: dec!(50000.0),
            trade_id: 0,
        };

        let order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: dec!(45000.0),
            quantity: dec!(3.0),
            time: 0,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        let trade = position.apply_order(&order, 0);
        assert!(trade.is_some());
        let trade = trade.unwrap();

        // Realized PnL for closing existing position
        assert_eq!(trade.realized_pnl, dec!(10000.0)); // (50000 - 45000) * 2
                                                       // Remaining position is long with qty = 1.0
        assert_eq!(position.qty, dec!(1.0));
        assert_eq!(position.side, Side::Bid);
        assert_eq!(position.avg_price, dec!(45000.0));
    }
}
