use tokio::task::JoinHandle;
use anyhow::{anyhow};
use async_std::sync::Arc;
use tokio::sync::RwLock;
use futures::{StreamExt, TryStreamExt};
use mongodb::options::{FindOptions};
use async_trait::async_trait;
use binance_q_mongodb::client::MongoClient;
use binance_q_types::{ATREntry, Sentiment, StudyConfig, StudyTypes, TfTrades, Candle};
use binance_q_utils::helpers::change_percent;

use mongodb::bson;
use mongodb::bson::doc;
use async_broadcast::Receiver;
use binance_q_events::EventSink;
use crate::Study;

#[derive(Clone)]
pub struct ATRStudy {
	config: Arc<StudyConfig>,
	tf_trades: Arc<RwLock<Receiver<TfTrades>>>,
	working: Arc<std::sync::RwLock<bool>>
}


impl ATRStudy {
	pub fn new(config: &StudyConfig, tf_trades: Receiver<TfTrades>) -> Self {
		Self {
			config: Arc::new(StudyConfig::from(config)),
			tf_trades: Arc::new(RwLock::new(tf_trades)),
			working: Arc::new(std::sync::RwLock::new(false)),
		}
		
	}
}
#[async_trait]
impl Study for ATRStudy {
	const ID: StudyTypes = StudyTypes::ATRStudy;
	type Entry = ATREntry;
	
	
	
	fn log_history(&self) -> JoinHandle<()> {
		let timeframes = vec![self.config.tf1, self.config.tf2, self.config.tf3];
		let config = self.config.clone();
		
		tokio::spawn( async move  {
			let mongo_client = MongoClient::new().await;
			if let Ok(past_trades) = mongo_client.tf_trades.find(None, None).await {
				let trades = past_trades.try_collect().await.unwrap_or_else(|_| vec![]);
				
				for tf in timeframes {
					let mut atr_entries: Vec<ATREntry> = vec![];
					
					let k = 2.0 / (config.range as f64 + 1.0);
					for span in trades.iter() {
						let id = span.id;
						if span.tf != tf {
							continue
						}
						let candle = Candle::from(span);
						let tr = candle.high - candle.low;
						let close_time =  span.trades.iter().map(|t| t.timestamp).max().unwrap();
						let v = if atr_entries.is_empty() {
							tr
						} else if candle.high == candle.low {
							atr_entries.last().unwrap().value
						} else {
							k * tr + (1.0 - k) * atr_entries.last().unwrap().value
						};
						let delta = change_percent(atr_entries.iter().map(|e| e.value).last().unwrap_or(tr), v);
						
						atr_entries.push(ATREntry {
							tf,
							value: v,
							delta,
							symbol: config.symbol.clone(),
							step_id: id,
							close_time,
						});
						

					}
					
					
					mongo_client.atr.insert_many(atr_entries, None).await.unwrap();
				}
				
			}
		})
	}
	
	async fn get_entry_for_tf(&self, _tf: u64) -> Self::Entry {
		todo!()
	}
	
	async fn get_n_entries_for_tf(&self, _n: u64, _tf: u64) -> Vec<Self::Entry> {
		todo!()
	}
	
	fn sentiment(&self) -> Sentiment {
		Sentiment::Neutral
	}
	fn sentiment_with_one<T>(&self, _other: T) -> Sentiment where T: Study {
		Sentiment::Bullish
	}
	
	fn sentiment_with_two<T, U>(&self, _other1: T, _other2: U) -> Sentiment where T: Study, U: Study {
		Sentiment::VeryBearish
	}
}

#[async_trait]
impl EventSink<TfTrades> for ATRStudy {
	fn get_receiver(&self) -> Arc<RwLock<Receiver<TfTrades>>> {
		self.tf_trades.clone()
	}
	fn set_working(&self, working: bool) -> anyhow::Result<()> {
		*self.working.write().unwrap() = working;
		Ok(())
	}
	fn working(&self) -> bool {
		self.working.read().unwrap().clone()
	}
	fn handle_event(&self, event_msg: TfTrades) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
		let config = self.config.clone();
		Ok(tokio::spawn(async move {
			let mongo_client = MongoClient::new().await;
			for trades in event_msg {
				let last_atrr = mongo_client.atr.find(doc! {"symbol": config.symbol.symbol.clone(), "tf": bson::to_bson(&trades.tf).unwrap()}, Some(FindOptions::builder().sort(doc! {
			"step_id": -1
		}).limit(1).build())).await?.next().await;
				
				if last_atrr.is_none() || last_atrr.clone().unwrap().is_err() {
					return Err(anyhow!("No last atrr found"));
				}
				let last_atr = last_atrr.unwrap()?;
				let k = 2.0 / (config.range as f64 + 1.0);
				let candle = Candle::from(&trades);
				let tr = candle.high - candle.low;
				let atr_value = if tr != 0.0 {
					k  * tr + (1.0 - k) * last_atr.value
				} else {
					last_atr.value
				};
				let delta = ((atr_value - last_atr.value) * 100.0) / last_atr.value;
				let close_time = trades.trades.iter().map(|t| t.timestamp).max().unwrap();
				
				let atr_entry = ATREntry {
					tf: trades.tf,
					value: atr_value,
					delta,
					symbol: config.symbol.clone(),
					step_id: trades.id,
					close_time: close_time,
				};
				
				mongo_client.atr.insert_one(atr_entry, None).await?;
			}
			Ok(())
			
		}))
		
	}
}



