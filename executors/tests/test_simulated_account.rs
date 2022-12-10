use binance_q_executors::simulated::SimulatedAccount;

#[cfg(test)]
mod tests {
	use binance_q_types::{ExchangeId, Order, OrderStatus, OrderType, Side, Symbol};
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
			EventSink::<OrderStatus>::listen(&s_a);
		});
		
		order_statuses_channel.0.broadcast(OrderStatus::Pending(Order {
			id: 1,
			symbol: symbol.clone(),
			side: Side::Bid,
			price: Decimal::new(100, 1),
			quantity: Decimal::new(100, 1),
			time: 123,
			order_type: OrderType::Limit
		})).await.unwrap();
		let open_orders = simulated_account.get_open_orders(&symbol).await;
		let accounts = simulated_account.get_symbol_account(&symbol).await;
		println!("open_orders: {:?}", accounts);
		assert!(true);
		Ok(())
	}
}