use kanal::AsyncSender;
use crate::mongodb::models::TradeEntry;
use async_trait::async_trait;
use mongodb::bson::doc;
use crate::helpers::to_tf_chunks;
use crate::mongodb::client::MongoClient;
use crate::types::{TfTrade, TfTrades};


#[async-trait()]
pub trait EventEmitter {
	type EventType;
	fn subscribe(&mut self, sender: AsyncSender<Self::EventType>);
	async fn listen(&self);
}
pub struct TfTradeEmitter {
	pub subscribers: Vec<AsyncSender<TfTrades>>,
	pub tf: u64,
}

impl TfTradeEmitter {
	pub fn new(tf: u64) -> Self {
		Self {
			subscribers: Vec::new(),
			tf,
		}
	}
}
impl EventEmitter for TfTradeEmitter {
	type EventType = TfTrades;

	fn subscribe(&mut self, sender: AsyncSender<Self::EventType>) {
		self.subscribers.push(sender);
	}
	
	async fn listen(&self) {
		let mongo_client = MongoClient::new();
		let mut last_timestamp = 0_u64;
		let mut last_id = 1_u64;
		loop {
			let trades = mongo_client.trades.find(doc! {
				"timestamp": {
					"$gt": last_timestamp
				}
			}, None).await.unwrap_or_else(|_| vec![]);
			let tf_trades = to_tf_chunks(self.tf, trades).iter().map(|t| {
				let mut trades = t.clone();
				let trade = TfTrade {
					id: last_id,
					tf: self.tf,
					trades,
				};
				last_id += 1;
				trade
			}).collect::<Vec<_>>();
			if tf_trades.len() > 0 {
				last_timestamp = tf_trades.last().unwrap().trades.last().unwrap().timestamp;
				futures::future::join_all(self.subscribers.iter().map(|s| s.send(tf_trades.clone())).collect::<Vec<_>>());
			}
			tokio::time::sleep(std::time::Duration::from_secs(tf)).await;
			
		}
	}
}

