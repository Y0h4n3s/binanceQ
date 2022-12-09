use anyhow::anyhow;
use tokio::task::JoinHandle;

use futures::{StreamExt, TryStreamExt};
use async_trait::async_trait;
use mongodb::bson;
use kanal::AsyncReceiver;
use crate::{EventSink, StudyConfig, TfTradeEmitter};
use crate::helpers::to_tf_chunks;
use crate::mongodb::client::MongoClient;
use crate::mongodb::models::{ChoppinessIndexEntry};
use crate::studies::{RANGE, Sentiment, Study};
use crate::types::{Candle, TfTrades};
use async_std::sync::Arc;
use mongodb::bson::doc;
use mongodb::options::FindOptions;
use crate::events::EventResult;
use crate::studies::Sentiment::Bearish;

#[derive(Clone)]
pub struct ChoppinessStudy {
	config: StudyConfig,
	tf_trades: Arc<AsyncReceiver<TfTrades>>,
	
}

impl ChoppinessStudy {
	pub fn new(config: &StudyConfig, tf_trades: AsyncReceiver<TfTrades>) -> Self {
		Self {
			config: StudyConfig::from(config),
			tf_trades: Arc::new(tf_trades)
		}
		
	}
}

impl Study for ChoppinessStudy {
	const ID: crate::studies::StudyTypes = crate::studies::StudyTypes::ChoppinessStudy;
	type Entry = ChoppinessIndexEntry;
	

	fn log_history(&self) -> JoinHandle<()> {
		let timeframes = vec![self.config.tf1, self.config.tf2, self.config.tf3];
		let config = self.config.clone();
		tokio::spawn(async move {
			let mongo_client = MongoClient::new().await;
			if let Ok(past_trades) = mongo_client.tf_trades.find(None, None).await {
				let trades = past_trades.try_collect().await.unwrap_or_else(|_| vec![]);
				for tf in timeframes {
					let mut choppiness_entries: Vec<ChoppinessIndexEntry> = vec![];
					let true_ranges = trades.iter().map(|chunk| {
						let candle = Candle::from(chunk);
						let tr = candle.high - candle.low;
						(tr, candle.high, candle.low)
					}).collect::<Vec<(f64, f64, f64)>>();
					let mut i = 0;
					let mut prev_delta = 0.0;
					while i < true_ranges.len() {
						if i < config.range as usize {
							i += 1;
							continue
						}
						let mut tr_sum = 0.0;
						let mut j = i;
						while j > i - config.range as usize {
							tr_sum += true_ranges[j].0;
							j -= 1;
						}
						let mut high_sum = 0.0;
						let mut j = i;
						while j > i - config.range as usize {
							if high_sum < true_ranges[j].1 {
								high_sum = true_ranges[j].1;
							}
							j -= 1;
						}
						let mut low_sum = f64::MAX;
						let mut j = i;
						while j > i - config.range as usize {
							if low_sum > true_ranges[j].2 {
								low_sum = true_ranges[j].2;
							}
							j -= 1;
						}
						let mut highest_diff = high_sum - low_sum;
						if highest_diff == 0.0 {
							highest_diff = 1.0;
						}
						let choppiness_index = 100.0 * (tr_sum / highest_diff).log10() / (config.range as f64).log10();
						i += 1;
						let choppiness_entry = ChoppinessIndexEntry {
							symbol: config.symbol.clone(),
							step_id: i as u64,
							tf,
							value: choppiness_index,
							delta: ((choppiness_index - prev_delta) * 100.0) / prev_delta,
							close_time: trades[i - 1].trades.iter().map(|t| t.timestamp).max().unwrap(),
						};
						prev_delta = choppiness_index;
						choppiness_entries.push(choppiness_entry);
						
					}
					if choppiness_entries.len() == 0 {
						continue
					}
					
					mongo_client.choppiness.insert_many(choppiness_entries, None).await.unwrap();
				}
				
			}
		})
	}
	
	
	fn sentiment(&self) -> Sentiment {
		Sentiment::Neutral
	}
	fn sentiment_with_one<T>(&self, _other: T) -> Sentiment where T: Study {
		
		Bearish
	}
	
	fn sentiment_with_two<T, U>(&self, _other1: T, _other2: U) -> Sentiment where T: Study, U: Study {
		Sentiment::VeryBullish
	}
	
	fn get_entry_for_tf(&self, _tf: u64) -> Self::Entry {
		todo!()
	}
	
	fn get_n_entries_for_tf(&self, _n: u64, _tf: u64) -> Vec<Self::Entry> {
		todo!()
	}
}

#[async_trait]
impl EventSink<TfTrades> for ChoppinessStudy {
	fn get_receiver(&self) -> Arc<AsyncReceiver<TfTrades>> {
		self.tf_trades.clone()
	}
	
	async fn handle_event(&self, event_msg: TfTrades) -> EventResult {
		let symbol = self.config.clone().symbol.clone();
		let config = self.config.clone();
		Ok(tokio::spawn(async move {
			let mongo_client = MongoClient::new().await;
			for trades in event_msg {
				let trades_client = TfTradeEmitter::new(trades.tf);
				let until = if trades.id <= config.range as u64 {
					1_u64
				} else {
					trades.id - config.range as u64 + 1
				
				};
				let mut past_trades = trades_client.get_tf_trades_until(until).await?;
				past_trades.push(trades.clone());
				let last_chopp = mongo_client.choppiness.find(doc! {"symbol": symbol.clone(), "tf": bson::to_bson(&trades.tf)?}, Some(FindOptions::builder().sort(doc! {
			"step_id": -1
		}).limit(1).build())).await?.next().await;
				
				let last_chop = if last_chopp.is_none() || last_chopp.clone().unwrap().is_err() {
					ChoppinessIndexEntry::default()
				} else {
					last_chopp.unwrap()?
				};
				
				let true_ranges = past_trades.iter().map(|chunk| {
					let candle = Candle::from(chunk);
					let tr = candle.high - candle.low;
					(tr, candle.high, candle.low)
				}).collect::<Vec<(f64, f64, f64)>>();
				let tr_sum = true_ranges.iter().map(|tr| tr.0).sum::<f64>();
				let highest_diff = true_ranges.iter().map(|tr| tr.1 - tr.2).max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(0.0);
				let choppiness_index = if highest_diff > 0.0 {
					100.0 * (tr_sum / highest_diff).log10() / (config.range as f64).log10()
				} else {
					last_chop.value
				};
				let choppiness_entry = ChoppinessIndexEntry {
					symbol: config.symbol.clone(),
					step_id: trades.id,
					tf: trades.tf,
					value: choppiness_index,
					delta: ((choppiness_index - last_chop.value) * 100.0) / last_chop.value,
					close_time: trades.trades.iter().map(|t| t.timestamp).max().unwrap_or(last_chop.close_time + trades.tf * 1000),
				};
			
				mongo_client.choppiness.insert_one(choppiness_entry, None).await?;
			}
			Ok(())
		}))
		
	}
}

