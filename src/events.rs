use crate::helpers::to_tf_chunks;
use crate::mongodb::client::MongoClient;
use crate::types::TfTrades;
use async_std::sync::Arc;
use async_trait::async_trait;
use futures::TryStreamExt;
use kanal::{AsyncReceiver, AsyncSender};
use mongodb::bson;
use mongodb::bson::doc;
use std::time::Duration;
use tokio::task::JoinHandle;

use crate::mongodb::models::TfTrade;
use tokio::sync::RwLock;

pub type EventResult = anyhow::Result<JoinHandle<std::result::Result<(), anyhow::Error>>>;
#[async_trait]
pub trait EventSink<EventType: Send + 'static> {
    fn get_receiver(&self) -> Arc<AsyncReceiver<EventType>>;
    async fn handle_event(&self, event_msg: EventType) -> EventResult;
    async fn listen(&self) -> anyhow::Result<JoinHandle<()>> {
        let receiver = self.get_receiver();
        while let Ok(event) = receiver.recv().await {
            self.handle_event(event).await?;
        }
        Ok(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }))
    }
}

#[async_trait]
pub trait EventEmitter<'a, EventType: Send + Sync + 'static> {
    fn get_subscribers(&self) -> Arc<RwLock<Vec<AsyncSender<EventType>>>>;
    async fn subscribe(&mut self, sender: AsyncSender<EventType>) {
        let subs = self.get_subscribers();
        subs.write().await.push(sender);
    }
    async fn emit(&self) -> anyhow::Result<JoinHandle<()>>;
}
pub struct TfTradeEmitter {
    subscribers: Arc<RwLock<Vec<AsyncSender<TfTrades>>>>,
    pub tf: u64,
}

impl TfTradeEmitter {
    pub fn new(tf: u64) -> Self {
        Self {
            subscribers: Arc::new(RwLock::new(Vec::new())),
            tf,
        }
    }
    
    

    pub async fn get_tf_trades_until(&self, until: u64) -> anyhow::Result<Vec<TfTrade>> {
        let mongo_client = MongoClient::new().await;
        let trades = mongo_client
            .tf_trades
            .find(
                doc! {"tf": bson::to_bson(&self.tf)?, "id": {"$gte": bson::to_bson(&until)?}},
                None,
            )
            .await?;
        let trades = trades.try_collect::<TfTrades>().await?;
        Ok(trades)
    }
}
#[async_trait]
impl EventEmitter<'_, TfTrades> for TfTradeEmitter {
    fn get_subscribers(&self) -> Arc<RwLock<Vec<AsyncSender<TfTrades>>>> {
        self.subscribers.clone()
    }

    async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
        let mongo_client = MongoClient::new().await;
        let mut last_timestamp = 0_u64;
        let mut last_id = 1_u64;
        let tf = self.tf;
        let subscribers = self.subscribers.clone();
        Ok(tokio::spawn(async move {
            loop {
                if let Ok(t) = mongo_client
                    .trades
                    .find(
                        doc! {
                            "timestamp": {
                                "$gt": mongodb::bson::to_bson(&last_timestamp).unwrap()
                            }
                        },
                        None,
                    )
                    .await
                {
                    let trades = t.try_collect().await.unwrap_or_else(|_| vec![]);
                    let tf_trades = to_tf_chunks(tf, trades)
                        .iter()
                        .map(|t| {
                            let trades = t.clone();
                            let trade = TfTrade {
                                id: last_id,
                                tf: tf,
                                trades,
                            };
                            last_id += 1;
                            trade
                        })
                        .collect::<Vec<_>>();
                    if !tf_trades.is_empty() {
                        mongo_client
                              .tf_trades
                              .insert_many(tf_trades.clone(), None)
                              .await.unwrap();
                    }
                    if tf_trades.len() > 0 {
                        last_timestamp = tf_trades.last().unwrap().trades.last().unwrap().timestamp;
                        futures::future::join_all(
                            subscribers
                                .read()
                                .await
                                .iter()
                                .map(|s| s.send(tf_trades.clone()))
                                .collect::<Vec<_>>(),
                        )
                        .await;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(tf)).await;
            }
        }))
    }
}
