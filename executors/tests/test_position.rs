#[cfg(test)]
mod tests {
	use binance_q_types::{ExchangeId, Side, Symbol};
	use rust_decimal::Decimal;
	use binance_q_executors::Position;
	use super::*;
	#[tokio::test]
	async fn test_add_long() -> anyhow::Result<()> {
		let symbol =  Symbol {
			symbol: "TST/USDT".to_string(),
			exchange: ExchangeId::Simulated,
			base_asset_precision: 1,
			quote_asset_precision: 2
		};
		let mut position = Position::new(Side::Ask, symbol, Decimal::ZERO, Decimal::ZERO);
		
		Ok(())
	}
}