use std::thread::JoinHandle;
use binance::api::Binance;
use binance::futures::market::FuturesMarket;
use binance::futures::model::Trades::AllTrades;
use mongodb::bson::doc;
use mongodb::options::UpdateOptions;
use crate::{AccessKey, StudyConfig};
use crate::mongodb::client::MongoClient;
use crate::studies::{Indicator, Sentiment, Study};
use std::time::{Duration, UNIX_EPOCH};
use binance::api::Futures::AggTrades;
use binance::futures::model::AggTrade;
use binance::futures::model::AggTrades::AllAggTrades;
use mongodb::bson;
pub struct ATRStudy {
	market: FuturesMarket,
	key: AccessKey,
	config: StudyConfig,
	indicator: ATRIndicator,
}



impl Study for ATRStudy {
	const ID: crate::studies::StudyTypes = crate::studies::StudyTypes::ObStudy;
	type Change = (f64, f64);
	
	fn new(key: AccessKey, config: &StudyConfig) -> Self {
		Self {
			market: FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone())),
			key: key.clone(),
			config: StudyConfig::from(config),
			indicator: ATRIndicator::default()
		}
		
	}
	
	fn log_history(&self) -> JoinHandle<()> {
		todo!()
	}
	
	
	fn start_log(&self) -> Vec<JoinHandle<()>> {
		let mut handles = vec![];
		handles
		
		
	}
	
	fn get_change(&self) -> Self::Change {
		todo!()
	}
	
	fn sentiment(&self) -> Sentiment {
		Sentiment::Neutral
	}
	fn sentiment_with_one<T>(&self, other: T) -> Sentiment where T: Study {
		
		todo!()
	}
	
	fn sentiment_with_two<T, U>(&self, other1: T, other2: U) -> Sentiment where T: Study, U: Study {
		todo!()
	}
}


pub struct ATRIndicator {
	values: Vec<f64>,
}
impl Indicator for ATRIndicator {
	const ID: crate::studies::IndicatorTypes = crate::studies::IndicatorTypes::ATR;
	type ValueType = ();
	
	fn get_value(&self) -> Self::ValueType {
		todo!()
	}
}


impl Default for ATRIndicator {
	fn default() -> Self {
		Self {
			values: vec![]
		}
	}
}
