use std::thread::JoinHandle;
use std::time::{Duration, UNIX_EPOCH};
use binance::api::Binance;
use binance::futures::market::FuturesMarket;
use mongodb::bson::doc;
use mongodb::bson;
use mongodb::options::UpdateOptions;
use crate::{AccessKey, StudyConfig};
use crate::mongodb::client::MongoClient;
use crate::mongodb::models::Side;
use crate::studies::{Sentiment, Study};

pub struct OiStudy {
	market: FuturesMarket,
	key: AccessKey,
	config: StudyConfig
}




impl Study for OiStudy {
	const ID: crate::studies::StudyTypes = crate::studies::StudyTypes::OiStudy;
	type Change = (f64);
	
	fn new(key: AccessKey, config: &StudyConfig) -> Self {
		OiStudy {
			market: FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone())),
			key: key.clone(),
			config: StudyConfig::from(config)
		}
		
	}
	
	fn log_history(&self) -> JoinHandle<()> {
		todo!("OiStudy::log_history()")
	}
	fn start_log(&self) -> Vec<JoinHandle<()>> {
		let mut handles = vec![];
		
		// for tf in vec![self.config.tf1, self.config.tf2, self.config.tf3] {
		// 	handles.push(std::thread::spawn(move || {
		// 		let mongo_client = MongoClient::new();
		// 		loop {
		// 			let oi_result = self.market.open_interest(&self.config.symbol);
		// 			if let Ok(oi) = oi_result {
		// 				mongo_client.open_interest.update_one(doc! {
		// 			"timestamp": bson::to_bson(&std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()).unwrap(),
		// 		}, doc! {
		// 			"$set": {
		// 				"value": oi.open_interest,
		// 				"timestamp": bson::to_bson(&std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()).unwrap(),
		// 			}
		// 		},                             Some(UpdateOptions::builder().upsert(true).build())
		// 				).unwrap();
		// 				println!("OI: {:?}", oi);
		// 			}
		// 			std::thread::sleep(Duration::from_secs(tf));
		//
		//
		// 		}
		// 	}))
		// }
		return handles;
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