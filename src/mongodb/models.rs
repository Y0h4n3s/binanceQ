
use serde_with::*;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};



#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ATREntry {
    pub tf: u64,
    pub atr: f64,
    pub delta: f64,
    pub symbol: String,
    pub step_id: u64,
    pub close_time: u64
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BookSideEntry {
    pub value: f64,
    pub side: Side,
    pub timestamp: u64
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TradeEntry {
    pub id: u64,
    pub price: f64,
    pub qty: f64,
    pub timestamp: u64,
    pub delta: f64,
    pub symbol: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OpenInterestEntry {
    pub timestamp: u64,
    pub value: f64,
    
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Copy, Clone)]
pub struct TokenNode {

}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Side{
    Bid,
    Ask
    
    
}


