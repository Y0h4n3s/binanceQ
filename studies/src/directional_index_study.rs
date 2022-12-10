use tokio::task::JoinHandle;
use anyhow::anyhow;
use async_std::sync::Arc;
use mongodb::options::{FindOptions};
use async_trait::async_trait;
use binance_q_types::{ATREntry, StudyTypes, AverageDirectionalIndexEntry, StudyConfig, TfTrades, Sentiment, Candle, GlobalConfig};
use binance_q_utils::helpers::change_percent;
use async_broadcast::{Receiver, Sender};
use binance_q_mongodb::loader::TfTradeEmitter;
use binance_q_mongodb::client::MongoClient;
use kanal::AsyncReceiver;
use mongodb::bson;
use futures::{TryStreamExt, StreamExt};
use mongodb::bson::doc;
use binance_q_events::EventSink;
use yata::core::{IndicatorConfig, PeriodType, IndicatorInstance, IndicatorInstanceDyn};
use yata::indicators::AverageDirectionalIndex;
use yata::prelude::*;
use tokio::sync::RwLock;

use crate::Study;

#[derive(Clone)]
pub struct DirectionalIndexStudy {
	config: Arc<StudyConfig>,
	tf_trades: Arc<RwLock<Receiver<TfTrades>>>,
	working: Arc<std::sync::RwLock<bool>>
}


impl DirectionalIndexStudy {
	pub fn new(config: &StudyConfig, tf_trades: Receiver<TfTrades>) -> Self {
		Self {
			config: Arc::new(StudyConfig::from(config)),
			tf_trades: Arc::new(RwLock::new(tf_trades)),
			working: Arc::new(std::sync::RwLock::new(false)),
		}
		
	}
}

impl Study for DirectionalIndexStudy {
	const ID: StudyTypes = StudyTypes::DirectionalIndexStudy;
	type Entry = AverageDirectionalIndexEntry;
	
	
	
	fn log_history(&self) -> JoinHandle<()> {
		let timeframes = vec![self.config.tf1, self.config.tf2, self.config.tf3];
		let config = self.config.clone();
		
		tokio::spawn( async move  {
			let mongo_client = MongoClient::new().await;
			if let Ok(past_trades) = mongo_client.tf_trades.find(None, None).await {
				let trades = past_trades.try_collect().await.unwrap_or_else(|_| vec![]);
				for tf in timeframes {
					let mut adi = yata::indicators::AverageDirectionalIndex::default();
					let mut adi_instance = None;
					adi.method1 = ("ema-".to_string() + config.range.to_string().as_str()).parse().unwrap();
					adi.method2 = ("ema-".to_string() + config.range.to_string().as_str()).parse().unwrap();
					adi.period1 = config.range as u8;
					let mut prev_value = None;
					let mut adi_entries: Vec<AverageDirectionalIndexEntry> = vec![];
					for (i, span) in trades.iter().enumerate() {
						if span.tf != tf {
							continue
						}
						let candle = Candle::from(span);
						if i == 0 {
							adi_instance = Some(adi.init(&candle).unwrap());
							continue
						}
						let value = IndicatorInstance::next(adi_instance.as_mut().unwrap(), &candle);
						prev_value = Some(value);
						let delta = if prev_value.is_some() {
							change_percent(prev_value.unwrap().value(0), value.value(0))
						} else {
							0.0
						};
						let positive_delta = if prev_value.is_some() {
							change_percent(prev_value.unwrap().value(1), value.value(1))
						} else {
							0.0
						};
						let negative_delta = if prev_value.is_some() {
							change_percent(prev_value.unwrap().value(2), value.value(2))
						} else {
							0.0
						};
						adi_entries.push(AverageDirectionalIndexEntry {
							tf,
							value: value.value(0),
							positive: value.value(1),
							negative: value.value(0),
							delta,
							positive_delta,
							negative_delta,
							symbol: config.symbol.clone(),
							step_id: span.id,
							close_time: span.trades.iter().max_by(|a, b| a.timestamp.cmp(&b.timestamp)).unwrap().timestamp,
						});
					}
					
					mongo_client.adi.insert_many(adi_entries, None).await.unwrap();
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


impl EventSink<TfTrades> for DirectionalIndexStudy {
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
			
			let mut adi = yata::indicators::AverageDirectionalIndex::default();
			adi.method1 = ("ema-".to_string() + config.range.to_string().as_str()).parse().unwrap();
			adi.method2 = ("ema-".to_string() + config.range.to_string().as_str()).parse().unwrap();
			adi.period1 = config.range as u8;
			let mut adi_instance = adi.init(&Candle::default())?;
			let gc = GlobalConfig {
				symbol: config.symbol.clone(),
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
				let past_trades = trades_client.get_tf_trades_until(until).await?;
				for trade in past_trades {
					let candle = Candle::from(&trade);
					IndicatorInstance::next(&mut adi_instance, &candle);
				}
				let last_adii = mongo_client.adi.find(doc! {"symbol": config.symbol.symbol.clone(), "tf": bson::to_bson(&trades.tf).unwrap()}, Some(FindOptions::builder().sort(doc! {
			"step_id": -1
		}).limit(1).build())).await?.next().await;
				
				let prev_value = if last_adii.is_none() || last_adii.clone().unwrap().is_err() {
					AverageDirectionalIndexEntry::default()
				} else {
					last_adii.unwrap().unwrap()
				};
				
				let candle = Candle::from(&trades);
				let value = IndicatorInstance::next(&mut adi_instance, &candle);
				let delta = change_percent(prev_value.value, value.value(0));
				
				let positive_delta =
					change_percent(prev_value.positive, value.value(1));
				
				let negative_delta =
					change_percent(prev_value.negative, value.value(2));
				
				let adi_entry = AverageDirectionalIndexEntry {
					tf: trades.tf,
					value: value.value(0),
					positive: value.value(1),
					negative: value.value(2),
					delta,
					positive_delta,
					negative_delta,
					symbol: config.symbol.clone(),
					step_id: trades.id,
					close_time: trades.trades.iter().max_by(|a, b| a.timestamp.cmp(&b.timestamp)).unwrap().timestamp,
				};
				
				mongo_client.adi.insert_one(adi_entry, None).await?;
			}
			Ok(())
		}))
		
	}
}



