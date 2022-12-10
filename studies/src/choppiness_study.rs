use tokio::task::JoinHandle;
use async_broadcast::{Receiver};
use futures::{StreamExt, TryStreamExt};
use mongodb::bson;
use async_std::sync::Arc;
use binance_q_types::{ChoppinessIndexEntry, Sentiment, StudyConfig, TfTrades, StudyTypes, Candle, GlobalConfig};
use binance_q_utils::helpers::change_percent;
use binance_q_mongodb::client::MongoClient;
use binance_q_mongodb::loader::TfTradeEmitter;
use async_trait::async_trait;

use binance_q_types::Sentiment::Bearish;
use mongodb::bson::doc;
use tokio::sync::RwLock;

use mongodb::options::FindOptions;
use binance_q_events::EventSink;
use crate::Study;

#[derive(Clone)]
pub struct ChoppinessStudy {
	config: StudyConfig,
	tf_trades: Arc<RwLock<Receiver<TfTrades>>>,
	working: Arc<std::sync::RwLock<bool>>
}

impl ChoppinessStudy {
	pub fn new(config: &StudyConfig, tf_trades: Receiver<TfTrades>) -> Self {
		Self {
			config: StudyConfig::from(config),
			tf_trades: Arc::new(RwLock::new(tf_trades)),
			working: Arc::new(std::sync::RwLock::new(false)),
		}
		
	}
}

#[async_trait]
impl Study for ChoppinessStudy {
	const ID: StudyTypes = StudyTypes::ChoppinessStudy;
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
							delta: change_percent(prev_delta, choppiness_index),
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
	
	async fn get_entry_for_tf(&self, tf: u64) -> Self::Entry {
			let mongo_client = MongoClient::new().await;
			mongo_client.choppiness.find(
				doc! {
					"symbol": self.config.symbol.symbol.clone(),
					"tf": bson::to_bson(&tf).unwrap(),
				},
				FindOptions::builder().limit(1).build()
			).await.unwrap().next().await.unwrap().unwrap()
	}
	
	async fn get_n_entries_for_tf(&self, _n: u64, _tf: u64) -> Vec<Self::Entry> {
		todo!()
	}
}

impl EventSink<TfTrades> for ChoppinessStudy {
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
		let symbol = self.config.clone().symbol.clone();
		let config = self.config.clone();
		Ok(tokio::spawn(async move {
			let mongo_client = MongoClient::new().await;
			let gc = GlobalConfig {
				symbol: symbol.clone(),
				tf1: config.tf1,
				..Default::default()
			};
			for trades in event_msg {
				let trades_client = TfTradeEmitter::new(trades.tf, gc.clone());
				let until = if trades.id <= config.range as u64 {
					1_u64
				} else {
					trades.id - config.range as u64 + 1
				
				};
				let mut past_trades = trades_client.get_tf_trades_until(until).await?;
				past_trades.push(trades.clone());
				let last_chopp = mongo_client.choppiness.find(doc! {"symbol": symbol.symbol.clone(), "tf": bson::to_bson(&trades.tf)?}, Some(FindOptions::builder().sort(doc! {
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
					delta: change_percent(last_chop.value, choppiness_index),
					close_time: trades.trades.iter().map(|t| t.timestamp).max().unwrap_or(last_chop.close_time + trades.tf * 1000),
				};
			
				mongo_client.choppiness.insert_one(choppiness_entry, None).await?;
			}
			Ok(())
		}))
		
	}
}

