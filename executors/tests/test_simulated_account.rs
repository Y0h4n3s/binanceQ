use binance_q_executors::simulated::SimulatedAccount;

#[cfg(test)]
mod tests {
	use binance_q_types::{ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, TfTrade, TfTrades, TradeEntry};
	use rust_decimal::Decimal;
	use binance_q_events::{EventEmitter, EventSink};
	use binance_q_executors::ExchangeAccountInfo;
	use super::*;
	#[tokio::test]
	async fn test_simulated_account_add_pending_order() -> anyhow::Result<()> {
		let tf_trades_channel = async_broadcast::broadcast(100);
		let order_statuses_channel = async_broadcast::broadcast(100);
		
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let simulated_account = SimulatedAccount::new(
			tf_trades_channel.1,
			order_statuses_channel.1,
			vec![symbol.clone()]
		).await;
		let s_a = simulated_account.clone();
		std::thread::spawn(move || {
			EventSink::<OrderStatus>::listen(&simulated_account).unwrap();
		});
		let os = OrderStatus::Pending(Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(100, 1),
			time: 123,
			order_type: OrderType::Limit
		});
		order_statuses_channel.0.broadcast(os.clone()).await.unwrap();
		while order_statuses_channel.0.len() > 0 {
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
		
		let open_orders = s_a.get_open_orders(&symbol).await;
		assert_eq!(open_orders.len(), 1);
		assert!(open_orders.contains(&os));
		Ok(())
	}
	
	#[tokio::test]
	async fn test_simulated_account_fill_pending_order() -> anyhow::Result<()> {
		let tf_trades_channel = async_broadcast::broadcast(100);
		let order_statuses_channel = async_broadcast::broadcast(100);
		
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let simulated_account = SimulatedAccount::new(
			tf_trades_channel.1,
			order_statuses_channel.1,
			vec![symbol.clone()]
		).await;
		let s_a1 = simulated_account.clone();
		let s_a = simulated_account.clone();
		std::thread::spawn(move || {
			EventSink::<OrderStatus>::listen(&simulated_account).unwrap();
		});
		std::thread::spawn(move || {
			EventSink::<TfTrades>::listen(&s_a1).unwrap();
		});
		let os = OrderStatus::Pending(Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(100, 1),
			time: 123,
			order_type: OrderType::Limit
		});
		order_statuses_channel.0.broadcast(os.clone()).await.unwrap();
		while order_statuses_channel.0.len() > 0 {
			tokio::time::sleep(std::time::Duration::from_micros(100)).await;
		}
		
		tf_trades_channel.0.broadcast(vec![TfTrade {
			symbol: symbol.clone(),
			tf: 1,
			id: 1,
			timestamp: 124,
			trades: vec![TradeEntry {
				id: 1,
				price: 9.0,
				qty: 100.0,
				timestamp: 0,
				delta: 0.0,
				symbol: symbol.clone()
			}]
		}]).await.unwrap();
		while tf_trades_channel.0.len() > 0 {
			tokio::time::sleep(std::time::Duration::from_micros(100)).await;
		}
		
		let open_orders = s_a.get_open_orders(&symbol).await;
		assert_eq!(open_orders.len(), 0);
		Ok(())
	}
	
	#[tokio::test]
	async fn test_simulated_account_partial_fill_pending_order() -> anyhow::Result<()> {
		let tf_trades_channel = async_broadcast::broadcast(100);
		let order_statuses_channel = async_broadcast::broadcast(100);
		
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let simulated_account = SimulatedAccount::new(
			tf_trades_channel.1,
			order_statuses_channel.1,
			vec![symbol.clone()]
		).await;
		let s_a1 = simulated_account.clone();
		let s_a = simulated_account.clone();
		std::thread::spawn(move || {
			EventSink::<OrderStatus>::listen(&simulated_account).unwrap();
		});
		std::thread::spawn(move || {
			EventSink::<TfTrades>::listen(&s_a1).unwrap();
		});
		let os = OrderStatus::Pending(Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(100, 0),
			time: 123,
			order_type: OrderType::Limit
		});
		order_statuses_channel.0.broadcast(os.clone()).await.unwrap();
		while order_statuses_channel.0.len() > 0 {
			tokio::time::sleep(std::time::Duration::from_micros(100)).await;
		}
		
		tf_trades_channel.0.broadcast(vec![TfTrade {
			symbol: symbol.clone(),
			tf: 1,
			id: 1,
			timestamp: 124,
			trades: vec![TradeEntry {
				id: 1,
				price: 10.0,
				qty: 90.0,
				timestamp: 0,
				delta: 0.0,
				symbol: symbol.clone()
			}]
		}]).await.unwrap();
		while tf_trades_channel.0.len() > 0 {
			tokio::time::sleep(std::time::Duration::from_micros(100)).await;
		}
		
		let open_orders = s_a.get_open_orders(&symbol).await;
		assert_eq!(open_orders.len(), 1);
		match open_orders.iter().next().unwrap() {
			OrderStatus::PartiallyFilled(order, filled_qty) => {
				assert_eq!(order.id, 1);
				assert_eq!(filled_qty, &Decimal::new(90, 0));
			},
			_ => panic!("Expected PartiallyFilled")
		}
		Ok(())
	}
	
	
	#[tokio::test]
	async fn test_simulated_account_emits_trade() -> anyhow::Result<()> {
		let tf_trades_channel = async_broadcast::broadcast(100);
		let order_statuses_channel = async_broadcast::broadcast(100);
		let (trades_sender, mut trades_receiver) = async_broadcast::broadcast(100);
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let mut simulated_account = SimulatedAccount::new(
			tf_trades_channel.1,
			order_statuses_channel.1,
			vec![symbol.clone()]
		).await;
		simulated_account.subscribe(trades_sender).await;
		let s_a1 = simulated_account.clone();
		let s_a = simulated_account.clone();
		std::thread::spawn(move || {
			EventSink::<OrderStatus>::listen(&simulated_account).unwrap();
		});
		std::thread::spawn(move || {
			EventSink::<TfTrades>::listen(&s_a1).unwrap();
		});
		let os = OrderStatus::Filled(Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(100, 0),
			time: 123,
			order_type: OrderType::Market
		});
		
		let closing_order = OrderStatus::Filled(Order {
			id: 2,
			symbol: symbol.clone(),
			side: Side::Ask,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(100, 0),
			time: 123,
			order_type: OrderType::Market
		});
		order_statuses_channel.0.broadcast(os.clone()).await.unwrap();
		order_statuses_channel.0.broadcast(closing_order.clone()).await.unwrap();
		s_a.emit().await?;
		
		while order_statuses_channel.0.len() > 0 {
			tokio::time::sleep(std::time::Duration::from_micros(100)).await;
		}
		while tf_trades_channel.0.len() > 0 {
			tokio::time::sleep(std::time::Duration::from_micros(100)).await;
		}
		let trade = trades_receiver.recv().await.unwrap();
		assert_eq!(trade.realized_pnl, Decimal::ZERO);
		assert_eq!(trade.position_side, Side::Bid);
		assert_eq!(trade.side, Side::Ask);
		
		
		let open_orders = s_a.get_open_orders(&symbol).await;
		assert_eq!(open_orders.len(), 0);
		Ok(())
	}
}