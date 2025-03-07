use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use uuid::Uuid;
use yata::core::{ValueType, OHLCV};
pub type TfTrades = Vec<TfTrade>;

#[derive(Debug, Clone, Default)]
pub struct Candle {
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}




#[derive(Debug, Clone, Default)]
pub struct AccessKey {
    pub api_key: String,
    pub secret_key: String,
}
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub enum Mode {
    Live,
    #[default]
    Backtest,
}
#[derive(Debug, Clone, Default)]
pub struct GlobalConfig {
    pub key: AccessKey,
    pub verbose: bool,
    pub symbol: Symbol,
    pub mode: Mode,
}

#[derive(Clone, Hash, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct Symbol {
    pub symbol: String,
    pub exchange: ExchangeId,
    pub base_asset_precision: u32,
    pub quote_asset_precision: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ChoppinessIndexEntry {
    pub tf: u64,
    pub value: f64,
    pub delta: f64,
    pub symbol: Symbol,
    pub step_id: u64,
    pub close_time: u64,
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
    pub close_time: u64,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ATREntry {
    pub tf: u64,
    pub value: f64,
    pub delta: f64,
    pub symbol: Symbol,
    pub step_id: u64,
    pub close_time: u64,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BookSideEntry {
    pub tf: u64,
    pub delta: f64,
    pub symbol: Symbol,
    pub step_id: u64,
    pub value: f64,
    pub side: Side,
    pub timestamp: u64,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TradeEntry {
    pub id: u64,
    pub price: Decimal,
    pub qty: Decimal,
    pub timestamp: u64,
    pub delta: Decimal,
    pub symbol: String,
}

#[cfg(feature = "trades")]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TfTrade {
    pub symbol: Symbol,
    pub tf: u64,
    pub id: u64,
    pub timestamp: u64,
    pub min_trade_time: u64,
    pub max_trade_time: u64,
    pub trades: Vec<TradeEntry>,
}

#[cfg(feature = "candles")]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TfTrade {
    pub symbol: Symbol,
    pub open_time: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub close_time: u64,
    pub quote_volume: Decimal,
    pub count: u64,
    pub taker_buy_volume: Decimal,
    pub taker_buy_quote_volume: Decimal,
    pub ignore: u64,
    pub tf: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OpenInterestEntry {
    pub timestamp: u64,
    pub value: f64,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Copy, Clone)]
pub struct TokenNode {}

#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum Side {
    Bid,
    Ask,
}

pub enum StudyTypes {
    ATRStudy,
    ChoppinessStudy,
    DirectionalIndexStudy,
}
#[derive(Clone)]
pub struct StudyConfig {
    pub symbol: Symbol,
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
#[derive(Clone, Hash, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize, Debug, Default)]
pub enum ExchangeId {
    #[default]
    Simulated,
    Binance,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ExecutionCommand {
    ExecuteOrder(Order),
    OpenLongPosition(Symbol, f64),
    OpenShortPosition(Symbol, f64),
    CloseLongPosition(Symbol, f64),
    CloseShortPosition(Symbol, f64),
}

#[derive(Clone, Hash, Eq, Ord, PartialOrd, PartialEq, Debug, Serialize, Deserialize)]
pub enum OrderType {
    Unknown,
    Limit,
    Market,
    TakeProfit(uuid::Uuid),
    StopLoss(uuid::Uuid),
    Cancel(uuid::Uuid),
    CancelFor(uuid::Uuid),
    StopLossLimit,
    TakeProfitLimit,
    StopLossTrailing(uuid::Uuid, Decimal),
}

pub struct FromProtoOrderType {
    pub uuid: uuid::Uuid,
    pub my_type: i32,
    pub trailing: Decimal,
}

impl From<FromProtoOrderType> for OrderType {
    fn from(proto: FromProtoOrderType) -> Self {
        match proto.my_type {
            0 => OrderType::Unknown,
            1 => OrderType::Limit,
            2 => OrderType::Market,
            3 => OrderType::TakeProfit(proto.uuid),
            4 => OrderType::StopLoss(proto.uuid),
            5 => OrderType::StopLossTrailing(proto.uuid, proto.trailing),
            6 => OrderType::StopLossLimit,
            7 => OrderType::TakeProfitLimit,
            8 => OrderType::Cancel(proto.uuid),
            9 => OrderType::CancelFor(proto.uuid),
            _ => OrderType::Unknown,
        }
    }
}

#[derive(Clone, PartialEq)]
pub enum StrategyEdge {
    Long(Symbol, f64),
    Short(Symbol, f64),
    CloseLong(Symbol, f64),
    CloseShort(Symbol, f64),
    Neutral,
}

#[derive(Hash, Eq, Ord, PartialOrd, PartialEq, Clone, Debug)]
pub enum OrderStatus {
    Pending(Order),
    Filled(Order),
    PartiallyFilled(Order, Decimal),
    Canceled(Order, String),
}

impl OrderStatus {
    pub fn order(&self) -> Order {
        match self {
            OrderStatus::Pending(o)
            | OrderStatus::Filled(o)
            | OrderStatus::PartiallyFilled(o, _)
            | OrderStatus::Canceled(o, _) => o.clone(),
        }
    }
}

impl PartialOrd<Self> for Order {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Order {
    fn cmp(&self, other: &Self) -> Ordering {
        use OrderType::*;
        let self_priority = match self.order_type {
            Cancel(_) => 0,
            Market => 1,
            Limit => 2,
            TakeProfitLimit => 3,
            StopLossLimit => 4,
            _ => 5,
        };

        let other_priority = match other.order_type {
            Cancel(_) => 0,
            Market => 1,
            Limit => 2,
            TakeProfitLimit => 3,
            StopLossLimit => 4,
            _ => 5,
        };

        self_priority.cmp(&other_priority)
    }
}

impl PartialEq for Order {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.order_type == other.order_type && self.side == other.side
    }
}

impl Hash for Order {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.order_type.hash(state);
        self.side.hash(state);
    }
}

#[derive(Clone, Hash, Eq, Ord, PartialOrd, PartialEq, Debug, Serialize, Deserialize)]
pub struct Kline {
    pub symbol: Symbol,
    pub open_time: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub close_time: u64,
    pub quote_volume: Decimal,
    pub count: u64,
    pub taker_buy_volume: Decimal,
    pub taker_buy_quote_volume: Decimal,
    pub ignore: u64,
}

impl From<binance::ws_model::Kline> for Kline {
    fn from(value: binance::ws_model::Kline) -> Self {
        let symbol = Symbol {
            symbol: value.symbol,
            ..Default::default()
        };
        Self {
            symbol,
            open_time: value.start_time as u64,
            open: Decimal::from_f64(value.open).unwrap(),
            high: Decimal::from_f64(value.high).unwrap(),
            low: Decimal::from_f64(value.low).unwrap(),
            close: Decimal::from_f64(value.close).unwrap(),
            volume: Decimal::from_f64(value.volume).unwrap(),
            close_time: value.end_time as u64,
            quote_volume: Decimal::from_f64(value.quote_volume).unwrap(),
            count: value.number_of_trades as u64,
            taker_buy_volume: Decimal::from_f64(value.active_buy_volume).unwrap(),
            taker_buy_quote_volume: Decimal::from_f64(value.active_volume_buy_quote).unwrap(),
            ignore: 0,
        }
    }
}
#[derive(Clone, Hash, Eq, Ord, PartialOrd, PartialEq, Debug, Serialize, Deserialize)]
pub struct Trade {
    pub id: u64,
    pub order_id: uuid::Uuid,
    pub symbol: Symbol,
    pub maker: bool,
    pub price: Decimal,
    pub commission: Decimal,
    pub position_side: Side,
    pub side: Side,
    pub realized_pnl: Decimal,
    pub exit_order_type: OrderType,
    pub qty: Decimal,
    pub quote_qty: Decimal,
    pub time: u64,
}

#[derive(Clone, Hash, Eq, Ord, PartialOrd, PartialEq, Debug, Serialize, Deserialize)]
pub enum ClosePolicy {
    None,
    BreakEven,
    BreakEvenOrMarketClose,
    ImmediateMarket,
}

impl From<i32> for ClosePolicy {
    fn from(i: i32) -> Self {
        match i {
            1 => ClosePolicy::BreakEven,
            2 => ClosePolicy::BreakEvenOrMarketClose,
            3 => ClosePolicy::ImmediateMarket,
            _ => ClosePolicy::BreakEven,
        }
    }
}

#[derive(Clone, Eq, Debug, Serialize, Deserialize)]
pub struct Order {
    pub id: uuid::Uuid,
    pub symbol: Symbol,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub time: u64,
    pub order_type: OrderType,
    pub lifetime: u64,
    pub close_policy: ClosePolicy,
}

impl Order {
    pub fn to_markdown_message(&self) -> String {
        let order_type = match self.order_type {
            OrderType::Market => "M",
            OrderType::TakeProfit(_) => "TP",
            OrderType::StopLoss(_) => "SL",
            OrderType::Limit => "L",
            OrderType::Cancel(_) => "C",
            OrderType::StopLossTrailing(_, _) => "SLT",
            OrderType::TakeProfitLimit => "TPL",
            OrderType::StopLossLimit => "SLL",
            OrderType::CancelFor(_) => "CF",
            OrderType::Unknown => "U",
        };
        format!(
            "*Symbol*: {}\nSide: {:?}\nPrice: {}\\.{}\nQty: {}\\.{}\nOrderType: {}",
            self.symbol.symbol,
            self.side,
            self.price
                .to_string()
                .split(".")
                .collect::<Vec<&str>>()
                .first()
                .unwrap(),
            self.price
                .to_string()
                .split(".")
                .collect::<Vec<&str>>()
                .get(1)
                .unwrap_or(&"0"),
            self.quantity
                .to_string()
                .split(".")
                .collect::<Vec<&str>>()
                .first()
                .unwrap(),
            self.quantity
                .to_string()
                .split(".")
                .collect::<Vec<&str>>()
                .get(1)
                .unwrap_or(&"0"),
            order_type
        )
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct SymbolAccount {
    pub symbol: Symbol,
    pub base_asset_free: Decimal,
    pub base_asset_locked: Decimal,
    pub quote_asset_free: Decimal,
    pub quote_asset_locked: Decimal,
}
