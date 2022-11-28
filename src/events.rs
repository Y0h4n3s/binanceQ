use kanal::{AsyncReceiver, AsyncSender};
use crate::mongodb::models::TradeEntry;
use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::Cursor;
use crate::helpers::to_tf_chunks;
use crate::mongodb::client::MongoClient;
use crate::types::{TfTrade, TfTrades};

#[async_trait]
pub trait EventSink<'a, EventType: Send + Clone + 'a> {
	fn get_receiver(&self) -> AsyncReceiver<EventType>;
	async fn handle_event(&self, event_msg: EventType);
	async fn listen(&'a self) {
		let receiver = self.get_receiver();
		while let Ok(event) = receiver.recv().await {
			self.handle_event(event).await;
		}
	}
}

#[async_trait]
pub trait EventEmitter<EventType> {
	fn subscribe(&mut self, sender: AsyncSender<EventType>);
	async fn emit(&self);
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
#[async_trait]
impl EventEmitter<TfTrades> for TfTradeEmitter {

	fn subscribe(&mut self, sender: AsyncSender<TfTrades>) {
		self.subscribers.push(sender);
	}
	
	async fn emit(&self) {
		let mongo_client = MongoClient::new().await;
		let mut last_timestamp = 0_u64;
		let mut last_id = 1_u64;
		loop {
			if let Ok(t) = mongo_client.trades.find(doc! {
				"timestamp": {
					"$gt": mongodb::bson::to_bson(&last_timestamp).unwrap()
				}
			}, None).await {
				let trades = t.try_collect().await.unwrap_or_else(|_| vec![]);
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
			}
			tokio::time::sleep(std::time::Duration::from_secs(self.tf)).await;
			
		}
	}
}

