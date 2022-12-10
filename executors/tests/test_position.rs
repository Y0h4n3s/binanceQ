#[cfg(test)]
mod tests {
	use binance_q_types::{ExchangeId, Order, OrderType, Side, Symbol};
	use rust_decimal::Decimal;
	use binance_q_executors::Position;
	#[tokio::test]
	async fn test_add_long() -> anyhow::Result<()> {
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
		let order_1 = Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(1, 0),
			time: 1,
			order_type: OrderType::Limit
		};
		let order_2 = Order {
			id: 2,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(200, 1),
			quantity: Decimal::new(2, 0),
			time: 2,
			order_type: OrderType::Limit
		};
		let no_trade = position.apply_order(&order_1);
		let no_trade_1 = position.apply_order(&order_2);
		assert!(no_trade.is_none());
		assert!(no_trade_1.is_none());
		assert!(position.is_long());
		assert_eq!(position.qty, Decimal::new(3, 0));
		
		Ok(())
	}
	
	#[tokio::test]
	async fn test_add_short() -> anyhow::Result<()> {
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
		let order_1 = Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Ask,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(1, 0),
			time: 1,
			order_type: OrderType::Limit
		};
		let order_2 = Order {
			id: 2,
			symbol: symbol.clone(),
			side: Side::Ask,
			price: Decimal::new(200, 1),
			quantity: Decimal::new(2, 0),
			time: 2,
			order_type: OrderType::Limit
		};
		let no_trade = position.apply_order(&order_1);
		let no_trade_1 = position.apply_order(&order_2);
		assert!(no_trade.is_none());
		assert!(no_trade_1.is_none());
		assert!(position.is_short());
		assert_eq!(position.qty, Decimal::new(3, 0));
		
		Ok(())
	}
	
	#[tokio::test]
	async fn test_close_long_with_profit() -> anyhow::Result<()> {
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
		let order_1 = Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(1, 0),
			time: 1,
			order_type: OrderType::Limit
		};
		let order_2 = Order {
			id: 2,
			symbol: symbol.clone(),
			side: Side::Ask,
			price: Decimal::new(200, 1),
			quantity: Decimal::new(1, 0),
			time: 2,
			order_type: OrderType::Limit
		};
		let no_trade = position.apply_order(&order_1);
		let no_trade_1 = position.apply_order(&order_2);
		assert!(no_trade.is_none());
		assert!(no_trade_1.is_some());
		assert!(!position.is_open());
		assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(10, 0));
		
		Ok(())
	}
	#[tokio::test]
	async fn test_close_long_with_loss() -> anyhow::Result<()> {
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
		let order_1 = Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Ask,
			price: Decimal::new(200, 1),
			quantity: Decimal::new(1, 0),
			time: 1,
			order_type: OrderType::Limit
		};
		let order_2 = Order {
			id: 2,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(1, 0),
			time: 2,
			order_type: OrderType::Limit
		};
		let no_trade = position.apply_order(&order_1);
		let no_trade_1 = position.apply_order(&order_2);
		assert!(no_trade.is_none());
		assert!(no_trade_1.is_some());
		assert!(!position.is_open());
		assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(-10, 0));
		
		Ok(())
	}
	
	#[tokio::test]
	async fn test_close_short_with_profit() -> anyhow::Result<()> {
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
		let order_1 = Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Ask,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(1, 0),
			time: 1,
			order_type: OrderType::Limit
		};
		let order_2 = Order {
			id: 2,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(200, 1),
			quantity: Decimal::new(1, 0),
			time: 2,
			order_type: OrderType::Limit
		};
		let no_trade = position.apply_order(&order_1);
		let no_trade_1 = position.apply_order(&order_2);
		assert!(no_trade.is_none());
		assert!(no_trade_1.is_some());
		assert!(!position.is_open());
		assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(10, 0));
		
		Ok(())
	}
	
	#[tokio::test]
	async fn test_close_short_with_loss() -> anyhow::Result<()> {
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
		let order_1 = Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Ask,
			price: Decimal::new(200, 1),
			quantity: Decimal::new(1, 0),
			time: 1,
			order_type: OrderType::Limit
		};
		let order_2 = Order {
			id: 2,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(1, 0),
			time: 2,
			order_type: OrderType::Limit
		};
		let no_trade = position.apply_order(&order_1);
		let no_trade_1 = position.apply_order(&order_2);
		assert!(no_trade.is_none());
		assert!(no_trade_1.is_some());
		assert!(!position.is_open());
		assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(-10, 0));
		
		Ok(())
	}
	#[tokio::test]
	async fn test_partial_close_long_with_profit() -> anyhow::Result<()> {
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
		let order_1 = Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(2, 0),
			time: 1,
			order_type: OrderType::Limit
		};
		let order_2 = Order {
			id: 2,
			symbol: symbol.clone(),
			side: Side::Ask,
			price: Decimal::new(200, 1),
			quantity: Decimal::new(1, 0),
			time: 2,
			order_type: OrderType::Limit
		};
		let no_trade = position.apply_order(&order_1);
		let no_trade_1 = position.apply_order(&order_2);
		assert!(no_trade.is_none());
		assert!(no_trade_1.is_some());
		assert!(position.is_open());
		assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(10, 0));
		
		Ok(())
	}
	#[tokio::test]
	async fn test_partial_close_long_with_loss() -> anyhow::Result<()> {
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
		let order_1 = Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Ask,
			price: Decimal::new(200, 1),
			quantity: Decimal::new(2, 0),
			time: 1,
			order_type: OrderType::Limit
		};
		let order_2 = Order {
			id: 2,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(1, 0),
			time: 2,
			order_type: OrderType::Limit
		};
		let no_trade = position.apply_order(&order_1);
		let no_trade_1 = position.apply_order(&order_2);
		assert!(no_trade.is_none());
		assert!(no_trade_1.is_some());
		assert!(position.is_open());
		assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(-10, 0));
		
		Ok(())
	}
	
	#[tokio::test]
	async fn test_partial_close_short_with_profit() -> anyhow::Result<()> {
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
		let order_1 = Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Ask,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(2, 0),
			time: 1,
			order_type: OrderType::Limit
		};
		let order_2 = Order {
			id: 2,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(200, 1),
			quantity: Decimal::new(1, 0),
			time: 2,
			order_type: OrderType::Limit
		};
		let no_trade = position.apply_order(&order_1);
		let no_trade_1 = position.apply_order(&order_2);
		assert!(no_trade.is_none());
		assert!(no_trade_1.is_some());
		assert!(position.is_open());
		assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(10, 0));
		
		Ok(())
	}
	
	#[tokio::test]
	async fn test_partial_close_short_with_loss() -> anyhow::Result<()> {
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let mut position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
		let order_1 = Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Ask,
			price: Decimal::new(200, 1),
			quantity: Decimal::new(2, 0),
			time: 1,
			order_type: OrderType::Limit
		};
		let order_2 = Order {
			id: 2,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(1, 0),
			time: 2,
			order_type: OrderType::Limit
		};
		let no_trade = position.apply_order(&order_1);
		let no_trade_1 = position.apply_order(&order_2);
		assert!(no_trade.is_none());
		assert!(no_trade_1.is_some());
		assert!(position.is_open());
		assert_eq!(no_trade_1.unwrap().realized_pnl, Decimal::new(-10, 0));
		
		Ok(())
	}
	
}