use tokio::task::JoinHandle;
use anyhow::anyhow;
use async_std::sync::Arc;
use futures::{StreamExt, TryStreamExt};
use mongodb::options::{FindOptions};
use async_trait::async_trait;
use kanal::AsyncReceiver;
use mongodb::bson;
use mongodb::bson::doc;
use crate::{EventSink, StudyConfig};
use crate::events::EventResult;
use crate::helpers::to_tf_chunks;
use crate::mongodb::client::MongoClient;
use crate::mongodb::models::ATREntry;
use crate::studies::{RANGE, Sentiment, Study};
use crate::types::{Candle, TfTrades};
#[derive(Clone)]
pub struct ATRStudy {
	config: Arc<StudyConfig>,
	tf_trades: Arc<AsyncReceiver<TfTrades>>,
	
}


impl ATRStudy {
	pub fn new(config: &StudyConfig, tf_trades: AsyncReceiver<TfTrades>) -> Self {
		Self {
			config: Arc::new(StudyConfig::from(config)),
			tf_trades: Arc::new(tf_trades),
		}
		
	}
}

impl Study for ATRStudy {
	const ID: crate::studies::StudyTypes = crate::studies::StudyTypes::ATRStudy;
	type Entry = ATREntry;
	
	
	
	fn log_history(&self) -> JoinHandle<()> {
		let timeframes = vec![self.config.tf1, self.config.tf2, self.config.tf3];
		let symbol = self.config.symbol.clone();
		
		tokio::spawn( async move  {
			let mongo_client = MongoClient::new().await;
			if let Ok(past_trades) = mongo_client.trades.find(None, None).await {
				let trades = past_trades.try_collect().await.unwrap_or_else(|_| vec![]);
				
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
					
					mongo_client.atr.insert_many(atr_entries, None).await.unwrap();
				}
				
			}
		})
	}
	
	fn get_entry_for_tf(&self, _tf: u64) -> Self::Entry {
		todo!()
	}
	
	fn get_n_entries_for_tf(&self, _n: u64, _tf: u64) -> Vec<Self::Entry> {
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
	fn get_receiver(&self) -> Arc<AsyncReceiver<TfTrades>> {
		self.tf_trades.clone()
	}
	
	async fn handle_event(&self, event_msg: TfTrades) -> EventResult {
		let symbol = self.config.clone().symbol.clone();
		Ok(tokio::spawn(async move {
			let mongo_client = MongoClient::new().await;
			for trades in event_msg {
				let last_atrr = mongo_client.atr.find(doc! {"symbol": symbol.clone(), "tf": bson::to_bson(&trades.tf).unwrap()}, Some(FindOptions::builder().sort(doc! {
			"step_id": -1
		}).limit(1).build())).await?.next().await;
				
				if last_atrr.is_none() || last_atrr.clone().unwrap().is_err() {
					return Err(anyhow!("No last atrr found"));
				}
				let last_atr = last_atrr.unwrap()?;
				let k = 2.0 / (RANGE as f64 + 1.0);
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
					symbol: symbol.clone(),
					step_id: trades.id,
					close_time: close_time,
				};
				
				mongo_client.atr.insert_one(atr_entry, None).await?;
			}
			Ok(())
		}))
		
	}
}



