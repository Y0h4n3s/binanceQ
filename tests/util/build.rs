use binance_q::managers::RiskManager;
pub fn build_risk_manager() -> RiskManager {
	let mut risk_manager = RiskManager::new(
		self.global_config.clone(),
		RiskManagerConfig {
			max_daily_losses: 100,
			max_risk_per_trade: 0.01,
		},
		trades_channel.1.clone(),
		execution_commands_channel.1.clone(),
		inner_account,
	);
	
}