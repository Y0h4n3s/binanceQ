use tokio::task::JoinHandle;
use std::time::{Duration, UNIX_EPOCH};

use binance::api::Binance;
use binance::futures::market::FuturesMarket;
use futures::{TryFutureExt, TryStreamExt};
use mongodb::bson;
use mongodb::bson::doc;
use async_trait::async_trait;
use crate::{AccessKey, StudyConfig};
use crate::helpers::to_tf_chunks;
use crate::mongodb::client::MongoClient;
use crate::mongodb::models::{ChoppinessIndexEntry};
use crate::studies::{RANGE, Sentiment, Study};

pub struct ChoppinessStudy {
	market: FuturesMarket,
	key: AccessKey,
	config: StudyConfig,
}

impl ChoppinessStudy {
}

#[async_trait]
impl Study for ChoppinessStudy {
	const ID: crate::studies::StudyTypes = crate::studies::StudyTypes::ChoppinessStudy;
	type Change = (f64, f64);
	
	fn new(key: AccessKey, config: &StudyConfig) -> Self {
		Self {
			market: FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone())),
			key: key.clone(),
			config: StudyConfig::from(config),
		}
		
	}
	
	async fn log_history(&self) -> JoinHandle<()> {
		let timeframes = vec![self.config.tf1, self.config.tf2, self.config.tf3];
		let symbol = self.config.symbol.clone();
		tokio::spawn(async move {
			let mongo_client = MongoClient::new().await;
			if let Ok(mut past_trades) = mongo_client.trades.find(None, None).await {
				let mut trades = past_trades.try_collect().await.unwrap_or_else(|_| vec![]);
				for tf in timeframes {
					let mut choppiness_entries: Vec<ChoppinessIndexEntry> = vec![];
					let tf_trades =  to_tf_chunks(tf, trades.clone());
					let true_ranges = to_tf_chunks(tf, trades.clone()).iter().map(|chunk| {
						let mut high: f64 = 0.0;
						let mut low: f64 = f64::MAX;
						for trade in chunk.iter() {
							high = high.max(trade.price);
							low = low.min( trade.price);
						}
						let tr = high - low;
						(tr, chunk.iter().max_by(|a, b| a.price.partial_cmp(&b.price).unwrap()).unwrap().price, chunk.iter().min_by(|a, b| a.price.partial_cmp(&b.price).unwrap()).unwrap().price)
					}).collect::<Vec<(f64, f64, f64)>>();
					let mut i = 0;
					let mut prev_delta = 0.0;
					while i < true_ranges.len() {
						if i < RANGE as usize {
							i += 1;
							continue
						}
						let mut tr_sum = 0.0;
						let mut j = i;
						while j > i - RANGE as usize {
							tr_sum += true_ranges[j].0;
							j -= 1;
						}
						let mut high_sum = 0.0;
						let mut j = i;
						while j > i - RANGE as usize {
							if high_sum < true_ranges[j].1 {
								high_sum = true_ranges[j].1;
							}
							j -= 1;
						}
						let mut low_sum = f64::MAX;
						let mut j = i;
						while j > i - RANGE as usize {
							if low_sum > true_ranges[j].2 {
								low_sum = true_ranges[j].2;
							}
							j -= 1;
						}
						let mut highest_diff = high_sum - low_sum;
						if highest_diff == 0.0 {
							highest_diff = 1.0;
						}
						let choppiness_index = 100.0 * (tr_sum / highest_diff).log10() / (RANGE as f64).log10();
						i += 1;
						let choppiness_entry = ChoppinessIndexEntry {
							symbol: symbol.clone(),
							step_id: i as u64,
							tf,
							value: choppiness_index,
							delta: ((choppiness_index - prev_delta) * 100.0) / prev_delta,
							close_time: tf_trades[i - 1].iter().map(|t| t.timestamp).max().unwrap(),
						};
						prev_delta = choppiness_index;
						choppiness_entries.push(choppiness_entry);
						
					}
					if choppiness_entries.len() == 0 {
						continue
					}
					
					mongo_client.choppiness.insert_many(choppiness_entries, None).await;
				}
				
			}
		})
	}
	
	
	async fn start_log(&self) -> Vec<JoinHandle<()>> {
		let mut handles = vec![];
		handles.push(self.log_history().await);
		for tf in vec![self.config.tf1, self.config.tf2, self.config.tf3] {
			let symbol = self.config.symbol.clone();
			handles.push(tokio::spawn( async move {
				let mongo_client = MongoClient::new().await;
				let last_time = UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
				loop {
					
					let agg_trades = mongo_client.trades.find(doc! {"timestamp": {"$gt": bson::to_bson(&last_time).unwrap()}}, None).await;
					
					std::thread::sleep(Duration::from_secs(tf));
					
					
				}
				
			}))
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


