use std::cmp::{max, min};
use std::thread::JoinHandle;
use binance::api::Binance;
use binance::futures::market::FuturesMarket;
use binance::futures::model::Trades::AllTrades;
use mongodb::bson::doc;
use mongodb::options::{FindOptions, UpdateOptions};
use crate::{AccessKey, StudyConfig};
use crate::mongodb::client::MongoClient;
use crate::studies::{Indicator, RANGE, Sentiment, Study};
use std::time::{Duration, UNIX_EPOCH};
use binance::api::Futures::AggTrades;
use binance::futures::model::AggTrade;
use binance::futures::model::AggTrades::AllAggTrades;
use mongodb::bson;
use crate::helpers::to_tf_chunks;
use crate::mongodb::models::ATREntry;

pub struct ATRStudy {
	market: FuturesMarket,
	key: AccessKey,
	config: StudyConfig,
}



impl Study for ATRStudy {
	const ID: crate::studies::StudyTypes = crate::studies::StudyTypes::ObStudy;
	type Change = (f64, f64);
	
	fn new(key: AccessKey, config: &StudyConfig) -> Self {
		Self {
			market: FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone())),
			key: key.clone(),
			config: StudyConfig::from(config),
		}
		
	}
	
	fn log_history(&self) -> JoinHandle<()> {
		let timeframes = vec![self.config.tf1, self.config.tf2, self.config.tf3];
		let symbol = self.config.symbol.clone();
		std::thread::Builder::new().name("atr_study_log_history".to_string()).spawn(move || {
			let mongo_client = MongoClient::new();
			if let Ok(mut past_trades) = mongo_client.trades.find(None, None) {
				let mut trades = past_trades.try_collect().unwrap_or_else(|_| vec![]);
				
				for tf in timeframes {
					let mut atr_values = vec![];
					let k = 2.0 / (RANGE as f64 + 1.0);
					for span in to_tf_chunks(tf,  trades.clone()) {
						if span.len() <= 0 {
							continue;
						}
						let mut high: f64 = 0.0;
						let mut low: f64 = f64::MAX;
						for trade in span.iter() {
							high = high.max(trade.price);
							low = low.min( trade.price);
						}
						let tr = (high - low);
						let close_time =  span.iter().map(|t| t.timestamp).max().unwrap();
						if atr_values.is_empty() {
							atr_values.push((tr, close_time));
						} else if high == low {
							atr_values.push((atr_values.last().unwrap().clone().0, close_time));
						} else {
							atr_values.push((k * tr + (1.0 - k) * atr_values.last().unwrap().0, close_time));
						}

					}
					if atr_values.len() == 0 {
						continue
					}
					let mut atr_entries = vec![];
					for (i, (v, close_time)) in atr_values.iter().enumerate() {
						if i == 0 {
							continue;
						}
						let delta = ((atr_values[i].0 - atr_values[i-1].0) * 100.0) / atr_values[i-1].0;
						atr_entries.push(ATREntry {
							tf,
							value: *v,
							delta,
							symbol: symbol.clone(),
							step_id: i as u64,
							close_time: *close_time,
						})
					}
					
					mongo_client.atr.insert_many(atr_entries, None).unwrap();
				}
				
			}
		}).unwrap()
	}
	
	
	fn start_log(&self) -> Vec<JoinHandle<()>> {
		let mut handles = vec![];
		handles.push(self.log_history());
		for tf in vec![self.config.tf1, self.config.tf2, self.config.tf3] {
			let symbol = self.config.symbol.clone();
			handles.push(std::thread::Builder::new().name("atr_study_log".to_string()).spawn( move || {
				let mongo_client = MongoClient::new();
				let mut last_time = UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
				loop {
					
					let mut agg_trades = mongo_client.trades.find(doc! {"timestamp": {"$gt": bson::to_bson(&last_time).unwrap()}}, None).unwrap();
					let mut trades = agg_trades.try_collect().unwrap_or_else(|_| vec![]);
					let mut last_atrr = mongo_client.atr.find(doc! {"symbol": symbol.clone(), "tf": bson::to_bson(&tf).unwrap()}, Some(FindOptions::builder().sort(doc! {
						"step_id": -1
					}).limit(1).build())).unwrap().next();
					
					if last_atrr.is_none() || last_atrr.clone().unwrap().is_err() {
						std::thread::sleep(Duration::from_secs(tf));
						continue;
					}
					let last_atr = last_atrr.unwrap().unwrap();
					let mut atr_values = vec![];
					let k = 2.0 / (RANGE as f64 + 1.0);
					for span in to_tf_chunks(tf,  trades.clone()) {
						let mut high: f64 = 0.0;
						let mut low: f64 = f64::MAX;
						for trade in span.iter() {
							high = high.max(trade.price);
							low = low.min( trade.price);
						}
						let tr = (high - low);
						let close_time =  span.iter().map(|t| t.timestamp).max().unwrap();
						if high == low {
							atr_values.push((last_atr.value, close_time));
						} else {
							atr_values.push((k * tr + (1.0 - k) * last_atr.value, close_time));
						}
						
					}
					let mut atr_entries = vec![];
					let mut atr_i =
						last_atr.step_id + 1;
					
					for (i, (v, close_time)) in atr_values.iter().enumerate() {
						if i == 0 {
							continue;
						}
						let delta = ((atr_values[i].0 - atr_values[i-1].0) * 100.0) / atr_values[i-1].0;
						atr_entries.push(ATREntry {
							tf,
							value: *v,
							delta,
							symbol: symbol.clone(),
							step_id: atr_i as u64,
							close_time: *close_time,
						});
						atr_i += 1;
					}
					if atr_entries.len() == 0 {
						
						atr_entries.push(ATREntry {
							tf,
							value: last_atr.value,
							delta: last_atr.delta,
							symbol: symbol.clone(),
							step_id: last_atr.step_id + 1,
							close_time:  last_atr.close_time + tf * 1000,
						});
						last_time = last_atr.close_time + tf * 1000;
						
					} else {
						last_time = trades.iter().map(|t| t.timestamp).max().unwrap();
						
					}
				mongo_client.atr.insert_many(atr_entries, None).unwrap();
				std::thread::sleep(Duration::from_secs(tf));
					
					
				}
				
			}).unwrap())
		}
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


