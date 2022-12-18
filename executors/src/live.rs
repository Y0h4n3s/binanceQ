use crate::{ExchangeAccount, ExchangeAccountInfo, Position, Spread, TradeExecutor};
use async_broadcast::{Receiver, Sender};
use async_std::io::WriteExt;
use async_trait::async_trait;
use binance_q_events::{EventEmitter, EventSink};
use binance_q_mongodb::client::MongoClient;
use binance_q_types::{
	ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, SymbolAccount, TfTrades, Trade,
};
use std::cmp::max;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

type ArcMap<K, V> = Arc<RwLock<HashMap<K, V>>>;
type ArcSet<T> = Arc<RwLock<HashSet<T>>>;

pub struct BinanceLiveAccount {
	pub symbol_accounts: ArcMap<Symbol, SymbolAccount>,
	pub open_orders: ArcMap<Symbol, ArcSet<OrderStatus>>,
	pub order_history: ArcMap<Symbol, ArcSet<OrderStatus>>,
	pub trade_history: ArcMap<Symbol, ArcSet<Trade>>,
	trade_q: Arc<RwLock<VecDeque<Trade>>>,
	pub trade_subscribers: Arc<RwLock<Sender<Trade>>>,
	pub positions: ArcMap<Symbol, Arc<RwLock<Position>>>,
	tf_trades: Arc<RwLock<Receiver<TfTrades>>>,
	order_statuses: Arc<RwLock<Receiver<OrderStatus>>>,
	tf_trades_working: Arc<std::sync::RwLock<bool>>,
	order_statuses_working: Arc<std::sync::RwLock<bool>>,
	spreads: ArcMap<Symbol, Arc<RwLock<Spread>>>,
}


pub struct BinanceLiveExecutor {
	account: Arc<BinanceLiveAccount>,
	pub orders: Arc<RwLock<Receiver<Order>>>,
	order_status_q: Arc<RwLock<VecDeque<OrderStatus>>>,
	pub order_status_subscribers: Arc<RwLock<Sender<OrderStatus>>>,
	order_working: Arc<std::sync::RwLock<bool>>,
}

impl BinanceLiveAccount {
	pub async fn new(
		tf_trades: Receiver<TfTrades>,
		order_statuses: Receiver<OrderStatus>,
		symbols: Vec<Symbol>,
	) -> Self {
		let symbol_accounts = Arc::new(RwLock::new(HashMap::new()));
		let open_orders = Arc::new(RwLock::new(HashMap::new()));
		let order_history = Arc::new(RwLock::new(HashMap::new()));
		let trade_history = Arc::new(RwLock::new(HashMap::new()));
		let positions = Arc::new(RwLock::new(HashMap::new()));
		let spreads = Arc::new(RwLock::new(HashMap::new()));
		
		for symbol in symbols {
			let symbol_account = SymbolAccount {
				symbol: symbol.clone(),
				base_asset_free: Default::default(),
				base_asset_locked: Default::default(),
				quote_asset_free: Decimal::new(100000, 0),
				quote_asset_locked: Default::default(),
			};
			let position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
			let spread = Spread::new(symbol.clone());
			symbol_accounts
				  .write()
				  .await
				  .insert(symbol.clone(), symbol_account);
			open_orders
				  .write()
				  .await
				  .insert(symbol.clone(), Arc::new(RwLock::new(HashSet::new())));
			order_history
				  .write()
				  .await
				  .insert(symbol.clone(), Arc::new(RwLock::new(HashSet::new())));
			trade_history
				  .write()
				  .await
				  .insert(symbol.clone(), Arc::new(RwLock::new(HashSet::new())));
			positions
				  .write()
				  .await
				  .insert(symbol.clone(), Arc::new(RwLock::new(position)));
			spreads
				  .write()
				  .await
				  .insert(symbol.clone(), Arc::new(RwLock::new(spread)));
		}
		Self {
			symbol_accounts,
			open_orders,
			order_history,
			trade_history,
			positions,
			spreads,
			tf_trades: Arc::new(RwLock::new(tf_trades)),
			trade_q: Arc::new(RwLock::new(VecDeque::new())),
			trade_subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
			order_statuses: Arc::new(RwLock::new(order_statuses)),
			tf_trades_working: Arc::new(std::sync::RwLock::new(false)),
			order_statuses_working: Arc::new(std::sync::RwLock::new(false)),
		}
	}
}


