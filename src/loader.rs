use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use binance::api::Binance;
use binance::futures::market::FuturesMarket;
use binance::futures::model::{AggTrade};
use binance::futures::model::AggTrades::AllAggTrades;
use mongodb::results::InsertManyResult;

use crate::AccessKey;
use crate::mongodb::client::MongoClient;
use crate::mongodb::models::TradeEntry;

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
