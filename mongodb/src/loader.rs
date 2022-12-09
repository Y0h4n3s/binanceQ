use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use binance::api::Binance;
use binance::futures::market::FuturesMarket;
use binance::futures::model::{AggTrade};
use binance::futures::model::AggTrades::AllAggTrades;
use mongodb::results::InsertManyResult;
use binance_q_types::TradeEntry;


pub fn to_tf_chunks(tf: u64, mut data: Vec<TradeEntry>) -> Vec<Vec<TradeEntry>> {
    if data.len() <= 0 {
        return vec![];
    }
    data.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    
    let mut chunks = vec![];
    let mut timestamp = data[0].timestamp;
    let mut last_i = 0;
    for (i, trade) in data.clone().iter().enumerate() {
        if trade.timestamp - timestamp < tf * 1000 {
            continue
        }
        timestamp = trade.timestamp;
        chunks.push(data[last_i..i].to_vec());
        last_i = i;
    }
    if chunks.len() <= 0 {
        chunks.push(data);
    }
    chunks
    
}

pub async fn insert_trade_entries(
    trades: &Vec<AggTrade>,
    symbol: String,
) -> mongodb::error::Result<InsertManyResult> {
    let client = MongoClient::new().await;
    let mut entries = vec![];
    for (i, t) in trades.iter().enumerate() {
        if i == 0 {
            continue;
        }
        let delta = ((trades[i].price - trades[i - 1].price) * 100.0) / trades[i - 1].price;

        let entry = TradeEntry {
            id: t.agg_id,
            price: t.price,
            qty: t.qty,
            timestamp: t.time,
            symbol: symbol.clone(),
            delta,
        };
        entries.push(entry);
    }
    client.trades.insert_many(entries, None).await
}

pub async fn load_history(key: AccessKey, symbol: String, fetch_history_span: u64) {
    let market = FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));

    let starting_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let mut start_time = starting_time as u64 - fetch_history_span;
    loop {
        println!("Fetching history from {} to {}", start_time, starting_time);
        if start_time > starting_time as u64 {
            break;
        }
        let trades_result =
            market.get_agg_trades(symbol.clone(), None, Some(start_time), None, Some(1000)).await;
        if let Ok(t) = trades_result {
            match t {
                AllAggTrades(trades) => {
                    if trades.len() <= 2 {
                        continue;
                    }
                    if let Ok(_) = insert_trade_entries(&trades, symbol.clone()).await {
                        start_time = trades.last().unwrap().time + 1;
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// TODO: use websockets for this
pub async fn start_loader(key: AccessKey, symbol: String, tf1: u64) -> JoinHandle<()> {
    let market = FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
	tokio::spawn(async move {
        let mut last_id = None;
        let mut start_time = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                - tf1,
        );
        loop {
            let trades_result =
                market.get_agg_trades(&symbol, last_id, start_time, None, Some(1000)).await;
            if let Ok(t) = trades_result {
                match t {
                    AllAggTrades(trades) => {
                        if trades.len() <= 2 {
                            continue;
                        }
                        if let Ok(_) = insert_trade_entries(&trades, symbol.clone()).await {
                            last_id = Some(trades.last().unwrap().agg_id + 1);
                            // println!("{}: {} {:?}", tf1, first_id.agg_id,  last_id.unwrap());
                        }
                    }
                }
            }
            if start_time.is_some() {
                start_time = None;
            }
            tokio::time::sleep(Duration::from_secs(tf1)).await;
        }
    })
}


pub struct TfTradeEmitter {
    subscribers: Arc<RwLock<Vec<AsyncSender<TfTrades>>>>,
    pub tf: u64,
    global_config: GlobalConfig,
}

impl TfTradeEmitter {
    pub fn new(tf: u64, global_config: GlobalConfig) -> Self {
        Self {
            subscribers: Arc::new(RwLock::new(Vec::new())),
            tf,
            global_config,
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
        let config = self.global_config.clone();
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
                                  symbol: config.symbol.clone(),
                                  timestamp: std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs(),
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
