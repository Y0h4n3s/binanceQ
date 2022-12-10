use binance_q_executors::simulated::SimulatedAccount;

#[cfg(test)]
mod tests {
	use binance_q_types::{ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, TfTrade, TfTrades, TradeEntry};
	use rust_decimal::Decimal;
	use binance_q_events::EventSink;
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
			EventSink::<OrderStatus>::listen(&simulated_account);
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
			EventSink::<OrderStatus>::listen(&simulated_account);
		});
		std::thread::spawn(move || {
			EventSink::<TfTrades>::listen(&s_a1);
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
		println!("{:?}", open_orders);
		assert_eq!(open_orders.len(), 0);
		Ok(())
	}
}