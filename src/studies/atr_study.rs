use tokio::task::JoinHandle;
use std::time::{Duration, UNIX_EPOCH};
use async_std::sync::Arc;
use binance::api::Binance;
use binance::futures::market::FuturesMarket;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use mongodb::bson;
use mongodb::bson::doc;
use mongodb::options::{FindOptions};
use async_trait::async_trait;
use kanal::AsyncReceiver;
use crate::{AccessKey, EventSink, GlobalConfig, StudyConfig};
use crate::helpers::to_tf_chunks;
use crate::mongodb::client::MongoClient;
use crate::mongodb::models::ATREntry;
use crate::studies::{RANGE, Sentiment, Study};
use crate::types::TfTrades;
#[derive(Clone)]
pub struct ATRStudy {
	market: FuturesMarket,
	global_config: GlobalConfig,
	config: Arc<StudyConfig>,
	tf_trades: Arc<AsyncReceiver<TfTrades>>,
	
}


impl ATRStudy {
	pub fn new(global_config: GlobalConfig, config: &StudyConfig, tf_trades: AsyncReceiver<TfTrades>) -> Self {
		Self {
			market: FuturesMarket::new(Some(global_config.key.api_key.clone()), Some(global_config.key.secret_key.clone())),
			global_config,
			config: Arc::new(StudyConfig::from(config)),
			tf_trades: Arc::new(tf_trades),
		}
		
	}
}

#[async_trait]
impl Study for ATRStudy {
	const ID: crate::studies::StudyTypes = crate::studies::StudyTypes::ATRStudy;
	type Entry = ATREntry;
	
	
	
	async fn log_history(&self) -> JoinHandle<()> {
		let timeframes = vec![self.config.tf1, self.config.tf2, self.config.tf3];
		let symbol = self.config.symbol.clone();
		tokio::spawn( async move  {
			let mongo_client = MongoClient::new().await;
			if let Ok(mut past_trades) = mongo_client.trades.find(None, None).await {
				let mut trades = past_trades.try_collect().await.unwrap_or_else(|_| vec![]);
				
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
						let tr = high - low;
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
					
					mongo_client.atr.insert_many(atr_entries, None).await;
				}
				
			}
		})
	}
	
	fn get_entry_for_tf(&self, tf: u64) -> Self::Entry {
		todo!()
	}
	
	fn get_n_entries_for_tf(&self, n: u64, tf: u64) -> Vec<Self::Entry> {
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

#[async_trait]
impl EventSink<TfTrades> for ATRStudy {
	fn get_receiver(&self) -> Arc<AsyncReceiver<TfTrades>> {
		self.tf_trades.clone()
	}
	
	async fn handle_event(&self, event_msg: TfTrades) -> JoinHandle<()> {
		let symbol = self.config.clone().symbol.clone();
		tokio::spawn(async move {
			let mongo_client = MongoClient::new().await;
			for mut trades in event_msg {
				let mut last_atrr = mongo_client.atr.find(doc! {"symbol": symbol.clone(), "tf": bson::to_bson(&trades.tf).unwrap()}, Some(FindOptions::builder().sort(doc! {
			"step_id": -1
		}).limit(1).build())).await.unwrap().next().await;
				
				if last_atrr.is_none() || last_atrr.clone().unwrap().is_err() {
					return;
				}
				let last_atr = last_atrr.unwrap().unwrap();
				let mut atr_values = vec![];
				let k = 2.0 / (RANGE as f64 + 1.0);
				// TODO: use the candle struct here
				for span in to_tf_chunks(trades.tf, trades.trades.clone()) {
					let mut high: f64 = 0.0;
					let mut low: f64 = f64::MAX;
					for trade in span.iter() {
						high = high.max(trade.price);
						low = low.min(trade.price);
					}
					let tr = high - low;
					let close_time = span.iter().map(|t| t.timestamp).max().unwrap();
					if high == low {
						atr_values.push((last_atr.value, close_time));
					} else {
						atr_values.push((k * tr + (1.0 - k) * last_atr.value, close_time));
					}
				}
				let mut atr_entries = vec![];
				
				for (i, (v, close_time)) in atr_values.iter().enumerate() {
					if i == 0 {
						continue;
					}
					let delta = ((atr_values[i].0 - atr_values[i - 1].0) * 100.0) / atr_values[i - 1].0;
					atr_entries.push(ATREntry {
						tf: trades.tf,
						value: *v,
						delta,
						symbol: symbol.clone(),
						step_id: trades.id,
						close_time: *close_time,
					});
				}
				if atr_entries.len() == 0 {
					atr_entries.push(ATREntry {
						tf: trades.tf,
						value: last_atr.value,
						delta: last_atr.delta,
						symbol: symbol.clone(),
						step_id: trades.id,
						close_time: last_atr.close_time + trades.tf * 1000,
					});
				}
				mongo_client.atr.insert_many(atr_entries, None).await;
			}
		})
		
	}
}



