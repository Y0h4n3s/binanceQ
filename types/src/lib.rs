use yata::core::{OHLCV, ValueType};

pub type TfTrades = Vec<TfTrade>;

#[derive(Debug, Clone, Default)]
pub struct Candle {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl OHLCV for Candle {
    fn open(&self) -> ValueType {
        self.open
    }
    
    fn high(&self) -> ValueType {
        self.high
    }
    
    fn low(&self) -> ValueType {
        self.low
    }
    
    fn close(&self) -> ValueType {
        self.close
    }
    
    fn volume(&self) -> ValueType {
        self.volume
    }
}

impl From<Vec<TradeEntry>> for Candle {
    fn from(trades: Vec<TradeEntry>) -> Self{
        if trades.is_empty() {
            return Self::default();
        }
        let tf_trade_entry = TfTrade {
            symbol: trades.first().unwrap().symbol.clone(),
            tf: 1,
            id: 1,
            timestamp: trades.iter().map(|trade| trade.timestamp).min().unwrap_or(0),
            trades
        };
        Self::from(&tf_trade_entry)
    }
}

impl From<&TfTrade> for Candle {
    fn from(tf_trade: &TfTrade) -> Self {
        if tf_trade.trades.len() == 0 {
            return Candle::default()
        }
        Self {
            open: tf_trade.trades.iter().min_by(|a, b| a.timestamp.partial_cmp(&b.timestamp).unwrap()).unwrap().price,
            high: tf_trade.trades.iter().max_by(|a, b| a.price.partial_cmp(&b.price).unwrap()).unwrap().price,
            low: tf_trade.trades.iter().min_by(|a, b| a.price.partial_cmp(&b.price).unwrap()).unwrap().price,
            close: tf_trade.trades.iter().max_by(|a, b| a.timestamp.partial_cmp(&b.timestamp).unwrap()).unwrap().price,
            volume: tf_trade.trades.iter().map(|t| t.qty).reduce(|a, b| a + b).unwrap_or(0.0),
        }
    }
}


#[derive(Debug, Clone)]
pub struct AccessKey {
    pub(crate) api_key: String,
    pub(crate) secret_key: String,
}
#[derive(Debug, Clone)]
pub struct GlobalConfig {
    pub tf1: u64,
    pub tf2: u64,
    pub tf3: u64,
    pub key: AccessKey,
}

#[derive(Clone,Hash, Eq,Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct Symbol {
    pub symbol: Symbol,
    pub exchange: ExchangeId,
    pub base_asset_precision: u32,
    pub quote_asset_precision: u32,
}

use serde::{Deserialize, Serialize};
use serde_with::*;
use crate::types::Symbol;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ChoppinessIndexEntry {
    pub tf: u64,
    pub value: f64,
    pub delta: f64,
    pub symbol: Symbol,
    pub step_id: u64,
    pub close_time: u64
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AverageDirectionalIndexEntry {
    pub tf: u64,
    pub value: f64,
    pub positive: f64,
    pub negative: f64,
    pub delta: f64,
    pub positive_delta: f64,
    pub negative_delta: f64,
    pub symbol: Symbol,
    pub step_id: u64,
    pub close_time: u64
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ATREntry {
    pub tf: u64,
    pub value: f64,
    pub delta: f64,
    pub symbol: Symbol,
    pub step_id: u64,
    pub close_time: u64
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BookSideEntry {
    pub tf: u64,
    pub delta: f64,
    pub symbol: Symbol,
    pub step_id: u64,
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
    pub symbol: Symbol
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TfTrade {
    pub symbol: Symbol,
    pub tf: u64,
    pub id: u64,
    pub timestamp: u64,
    pub trades: Vec<TradeEntry>,
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

pub enum StudyTypes {
    ATRStudy,
    ChoppinessStudy,
    DirectionalIndexStudy,
}
#[derive(Clone)]
pub struct StudyConfig {
    pub symbol: String,
    pub range: u16,
    pub tf1: u64,
    pub tf2: u64,
    pub tf3: u64,
}
impl From<&StudyConfig> for StudyConfig {
    fn from(config: &StudyConfig) -> Self {
        StudyConfig {
            symbol: config.symbol.clone(),
            range: config.range,
            tf1: config.tf1,
            tf2: config.tf2,
            tf3: config.tf3,
        }
    }
}
pub enum Sentiment {
    VeryBullish,
    Bullish,
    Neutral,
    Bearish,
    VeryBearish,
}

pub enum ExchangeId {
    Simulated
}

#[derive(Clone)]
pub enum ExecutionCommand {
    OpenLongPosition(Symbol, f64),
    OpenShortPosition(Symbol, f64),
    CloseLongPosition(Symbol, f64),
    CloseShortPosition(Symbol, f64),
}

#[derive(Hash, Eq,Ord, PartialOrd, PartialEq, Clone)]
pub enum OrderType {
    Limit,
    Market,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
    StopLossTrailing,
}

#[derive(Hash, Eq, Ord, PartialOrd, PartialEq, Clone)]
pub enum OrderStatus {
    Pending(Order),
    Filled(Order),
    Canceled(Order, String),
}
#[derive(Clone,Hash, Eq,Ord, PartialOrd, PartialEq)]
pub struct Trade {
    pub id: u64,
    pub order_id: u64,
    pub symbol: Symbol,
    pub maker: bool,
    pub price: Decimal,
    pub commission: Decimal,
    pub position_side: Side,
    pub side: Side,
    pub realized_pnl: Decimal,
    pub qty: Decimal,
    pub quote_qty: Decimal,
    pub time: u64,
}

#[derive(Clone,Hash, Eq,Ord, PartialOrd, PartialEq)]
pub struct Order {
    pub id: u64,
    pub symbol: Symbol,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub time: u64,
    pub order_type: OrderType,
}

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct SymbolAccount {
    pub symbol: Symbol,
    pub base_asset_free: Decimal,
    pub base_asset_locked: Decimal,
    pub quote_asset_free: Decimal,
    pub quote_asset_locked: Decimal,
}



