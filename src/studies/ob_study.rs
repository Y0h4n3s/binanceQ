use std::thread::JoinHandle;
use binance::api::Binance;
use binance::futures::market::FuturesMarket;
use mongodb::bson::doc;
use mongodb::options::UpdateOptions;
use crate::{AccessKey, StudyConfig};
use crate::mongodb::client::MongoClient;
use crate::studies::{Sentiment, Study, Side};
use mongodb::bson;
use std::time::{Duration, UNIX_EPOCH};
pub struct ObStudy {
	market: FuturesMarket,
	key: AccessKey,
	config: StudyConfig
}



impl Study for ObStudy {
	const ID: crate::studies::StudyTypes = crate::studies::StudyTypes::ObStudy;
	type Change = (f64, f64);

	fn new(key: AccessKey, config: &StudyConfig) -> Self {
		Self {
			market: FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone())),
			key: key.clone(),
			config: StudyConfig::from(config)
		}

	}

	fn log_history(&self) -> JoinHandle<()> {
		todo!()
	}


	fn start_log(&self) -> Vec<JoinHandle<()>> {
		let mut handles = vec![];
		
		
		// loop {
		// 	let order_book_result = self.market.get_custom_depth(&self.config.symbol, 20);
		// 	if let Ok(order_book) = order_book_result {
		// 		let asks_volume = order_book.asks.iter().fold(0.0, |acc, x| acc + (x.price * x.qty));
		// 		let bids_volume = order_book.bids.iter().fold(0.0, |acc, x| acc + (x.price * x.qty));
		// 		println!("{} {}", asks_volume, bids_volume);
		// 		mongo_client.book_side.update_one(doc! {
		// 			"timestamp": bson::to_bson(&std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()).unwrap(),
		// 			"side": bson::to_bson(&Side::Bid).unwrap(),
		// 		}, doc! {
		// 			"$set": {
		// 				"value": bids_volume,
		// 				"side": bson::to_bson(&Side::Bid).unwrap(),
		// 				"timestamp": bson::to_bson(&std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()).unwrap(),
		// 			}
		// 		},                             Some(UpdateOptions::builder().upsert(true).build())
		// 		).unwrap();
		// 		mongo_client.book_side.update_one(doc! {
		// 			"timestamp": bson::to_bson(&std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()).unwrap(),
		// 			"side": bson::to_bson(&Side::Ask).unwrap(),
		// 		}, doc! {
		// 			"$set": {
		// 				"value": asks_volume,
		// 				"side": bson::to_bson(&Side::Ask).unwrap(),
		// 				"timestamp": bson::to_bson(&std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()).unwrap(),
		// 			}
		// 		},                             Some(UpdateOptions::builder().upsert(true).build())
		// 		).unwrap();
		// 	}
		//
		// 	// std::thread::sleep(Duration::from_secs(5));
		//
		// }

		return handles
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

