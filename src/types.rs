use crate::mongodb::models::TradeEntry;

pub type TfTrades = Vec<TfTrade>;

#[derive(Debug, Clone)]
pub struct TfTrade {
	pub tf: u64,
	pub id: u64,
	pub trades: Vec<TradeEntry>,
}
#[derive(Debug, Clone)]
pub struct AccessKey {
	pub(crate) api_key: String,
	pub(crate) secret_key: String,
}

pub struct GlobalConfig {
	pub tf1: u64,
	pub tf2: u64,
	pub tf3: u64,
	pub key: AccessKey,
}