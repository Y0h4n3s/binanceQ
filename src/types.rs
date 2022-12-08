use yata::core::{OHLCV, ValueType};
use crate::executors::ExchangeId;
use crate::mongodb::models::{TfTrade, TradeEntry};

pub type TfTrades = Vec<TfTrade>;

#[derive(Debug, Clone, Default)]
pub struct Candle {
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	pub volume: f64,
}

impl OHLCV for Candle {
	fn open(&self) -> ValueType {
		self.open
	}
	
	fn high(&self) -> ValueType {
		self.high
	}
	
	fn low(&self) -> ValueType {
		self.low
	}
	
	fn close(&self) -> ValueType {
		self.close
	}
	
	fn volume(&self) -> ValueType {
		self.volume
	}
}

impl From<Vec<TradeEntry>> for Candle {
	fn from(trades: Vec<TradeEntry>) -> Self{
		let tf_trade_entry = TfTrade {
			tf: 1,
			id: 1,
			timestamp: trades.iter().map(|trade| trade.timestamp).min().unwrap_or(0),
			trades
		};
		Self::from(&tf_trade_entry)
	}
}

impl From<&TfTrade> for Candle {
	fn from(tf_trade: &TfTrade) -> Self {
		if tf_trade.trades.len() == 0 {
			return Candle::default()
		}
		Self {
			open: tf_trade.trades.iter().min_by(|a, b| a.timestamp.partial_cmp(&b.timestamp).unwrap()).unwrap().price,
			high: tf_trade.trades.iter().max_by(|a, b| a.price.partial_cmp(&b.price).unwrap()).unwrap().price,
			low: tf_trade.trades.iter().min_by(|a, b| a.price.partial_cmp(&b.price).unwrap()).unwrap().price,
			close: tf_trade.trades.iter().max_by(|a, b| a.timestamp.partial_cmp(&b.timestamp).unwrap()).unwrap().price,
			volume: tf_trade.trades.iter().map(|t| t.qty).reduce(|a, b| a + b).unwrap_or(0.0),
		}
	}
}


#[derive(Debug, Clone)]
pub struct AccessKey {
	pub(crate) api_key: String,
	pub(crate) secret_key: String,
}
#[derive(Debug, Clone)]
pub struct GlobalConfig {
	pub tf1: u64,
	pub tf2: u64,
	pub tf3: u64,
	pub key: AccessKey,
}

#[derive(Clone,Hash, Eq,Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct Symbol {
	pub symbol: String,
	pub exchange: ExchangeId,
	pub base_asset_precision: u32,
	pub quote_asset_precision: u32,
}