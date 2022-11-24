// use binance::api::Binance;
// use binance::futures::market::FuturesMarket;
// use binance::futures::model::Trades::AllTrades;
// use mongodb::bson::doc;
// use mongodb::options::UpdateOptions;
// use crate::{AccessKey, StudyConfig};
// use crate::mongodb::client::MongoClient;
// use crate::studies::{Sentiment, Study};
// use std::time::{Duration, UNIX_EPOCH};
// use mongodb::bson;
// pub struct VolStudy {
// 	market: FuturesMarket,
// 	key: AccessKey,
// 	config: StudyConfig
// }
//
//
//
// impl Study for VolStudy {
// 	const ID: crate::studies::StudyTypes = crate::studies::StudyTypes::ObStudy;
// 	type Change = (f64, f64);
//
// 	fn new(key: AccessKey, config: &StudyConfig) -> Self {
// 		Self {
// 			market: FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone())),
// 			key: key.clone(),
// 			config: StudyConfig::from(config)
// 		}
//
// 	}
//
//
// 	fn start_log(&self)  {
// 		let mongo_client = MongoClient::new();
// 		let last_id = None;
// 		loop {
// 			let trades_result = self.market.get_historical_trades(&self.config.symbol, last_id, None);
// 			if let Ok(t) = trades_result {
// 				match t {
// 					AllTrades(trades) => {
// 						println!("Trades: {:?}", trades);
// 					}
// 				}
// 				// 	let asks_volume = order_book.asks.iter().fold(0.0, |acc, x| acc + (x.price * x.qty));
// 				// 	let bids_volume = order_book.bids.iter().fold(0.0, |acc, x| acc + (x.price * x.qty));
// 				// 	println!("{} {}", asks_volume, bids_volume);
// 				// 	mongo_client.book_side.update_one(doc! {
// 				// 		"timestamp": bson::to_bson(&std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs())?,
// 				// 		"side": bson::to_bson(&Side::Bid)?,
// 				// 	}, doc! {
// 				// 		"$set": {
// 				// 			"value": bids_volume,
// 				// 			"side": bson::to_bson(&Side::Bid)?,
// 				// 			"timestamp": bson::to_bson(&std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs())?,
// 				// 		}
// 				// 	},                             Some(UpdateOptions::builder().upsert(true).build())
// 				// 	).unwrap();
// 				// 	mongo_client.book_side.update_one(doc! {
// 				// 		"timestamp": bson::to_bson(&std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs())?,
// 				// 		"side": bson::to_bson(&Side::Ask)?,
// 				// 	}, doc! {
// 				// 		"$set": {
// 				// 			"value": asks_volume,
// 				// 			"side": bson::to_bson(&Side::Ask)?,
// 				// 			"timestamp": bson::to_bson(&std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs())?,
// 				// 		}
// 				// 	},                             Some(UpdateOptions::builder().upsert(true).build())
// 				// 	).unwrap();
// 				// }
// 				//
// 				// std::thread::sleep(Duration::from_secs(5));
// 			}
// 		}
//
//
// 	}
//
// 	fn get_change(&self) -> Self::Change {
// 		todo!()
// 	}
//
// 	fn sentiment(&self) -> Sentiment {
// 		Sentiment::Neutral
// 	}
// 	fn sentiment_with_one<T>(&self, other: T) -> Sentiment where T: Study {
//
// 		todo!()
// 	}
//
// 	fn sentiment_with_two<T, U>(&self, other1: T, other2: U) -> Sentiment where T: Study, U: Study {
// 		todo!()
// 	}
// }
//
