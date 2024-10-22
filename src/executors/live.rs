#[allow(unused)]
#[allow(dead_code)]
use crate::events::{EventEmitter, EventSink};
use crate::executors::{ExchangeAccount, ExchangeAccountInfo, Position, Spread, TradeExecutor};
use crate::types::{
    AccessKey, ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, SymbolAccount, TfTrades,
    Trade,
};
use async_broadcast::{Receiver, Sender};
use async_trait::async_trait;
use binance::api::Binance;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use std::collections::{VecDeque};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;

use crate::executors::notification::{Notification, Notifier, TelegramNotifier};
use binance::futures::account::{CustomOrderRequest, FuturesAccount, TimeInForce};
use binance::futures::market::FuturesMarket;
use dashmap::{DashMap, DashSet};
use uuid::Uuid;

type ArcMap<K, V> = Arc<DashMap<K, V>>;
type ArcSet<T> = Arc<DashSet<T>>;

#[derive(Clone)]
pub struct BinanceLiveAccount {
    pub account: Arc<RwLock<FuturesAccount>>,
    pub market: Arc<RwLock<FuturesMarket>>,
    pub notifier: Arc<RwLock<TelegramNotifier>>,
    pub symbol_accounts: Arc<DashMap<Symbol, SymbolAccount>>,
    pub open_orders: Arc<DashMap<Symbol, Arc<DashSet<Order>>>>,
    pub order_history: ArcMap<Symbol, ArcSet<OrderStatus>>,
    pub trade_history: ArcMap<Symbol, ArcSet<Trade>>,
    trade_q: Arc<RwLock<VecDeque<Trade>>>,
    pub trade_subscribers: Arc<RwLock<Sender<(Trade, Option<Arc<Notify>>)>>>,
    pub positions: Arc<DashMap<Symbol, Position>>,
    tf_trades: Receiver<(TfTrades, Option<Arc<Notify>>)>,
    order_statuses: Receiver<(OrderStatus, Option<Arc<Notify>>)>,
}

pub struct BinanceLiveExecutor {
    account: Arc<BinanceLiveAccount>,
    pub orders: Receiver<(Order, Option<Arc<Notify>>)>,
    order_status_q: Arc<RwLock<VecDeque<(OrderStatus, Option<Arc<Notify>>)>>>,
    pub order_status_subscribers: Arc<RwLock<Sender<(OrderStatus, Option<Arc<Notify>>)>>>,
}

impl BinanceLiveExecutor {
    pub async fn new(
        key: AccessKey,
        orders_rx: Receiver<(Order, Option<Arc<Notify>>)>,
        trades_rx: Receiver<(TfTrades, Option<Arc<Notify>>)>,
        symbols: Vec<Symbol>,
        _trades: Sender<(Trade, Option<Arc<Notify>>)>,
    ) -> Self {
        let order_statuses_channel = async_broadcast::broadcast(100);
        let account =
            Arc::new(BinanceLiveAccount::new(key, trades_rx, order_statuses_channel.1, symbols).await);
        // account.subscribe(trades).await;
        // account.emit().await;
        let ac = account.clone();
        std::thread::spawn(move || {
            EventSink::<TfTrades>::listen(ac).unwrap();
        });
        let ac = account.clone();
        std::thread::spawn(move || {
            EventSink::<OrderStatus>::listen(ac).unwrap();
        });
        Self {
            account,
            orders: orders_rx,
            order_status_q: Arc::new(RwLock::new(VecDeque::new())),
            order_status_subscribers: Arc::new(RwLock::new(order_statuses_channel.0)),
        }
    }
}

impl BinanceLiveAccount {
    pub async fn new(
        key: AccessKey,
        tf_trades: Receiver<(TfTrades, Option<Arc<Notify>>)>,
        order_statuses: Receiver<(OrderStatus, Option<Arc<Notify>>)>,
        symbols: Vec<Symbol>,
    ) -> Self {

        let symbol_accounts = Arc::new(DashMap::new());
        let open_orders = Arc::new(DashMap::new());
        let order_history = Arc::new(DashMap::new());
        let trade_history = Arc::new(DashMap::new());
        let positions = Arc::new(DashMap::new());

        let account = FuturesAccount::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
        let market = FuturesMarket::new(None, None);
        for symbol in symbols {
            let symbol_account = SymbolAccount {
                symbol: symbol.clone(),
                base_asset_free: Default::default(),
                base_asset_locked: Default::default(),
                quote_asset_free: Decimal::new(100000, 0),
                quote_asset_locked: Default::default(),
            };
            let position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
            symbol_accounts
                .insert(symbol.clone(), symbol_account);
            open_orders
                .insert(symbol.clone(), Arc::new(DashSet::new()));
            order_history
                .insert(symbol.clone(), Arc::new(DashSet::new()));
            trade_history
                .insert(symbol.clone(), Arc::new(DashSet::new()));
            positions
                .insert(symbol.clone(), position);
        }
        Self {
            symbol_accounts,
            open_orders,
            order_history,
            trade_history,
            positions,
            market: Arc::new(RwLock::new(market)),
            notifier: Arc::new(RwLock::new(TelegramNotifier::new())),
            tf_trades,
            account: Arc::new(RwLock::new(account)),
            trade_q: Arc::new(RwLock::new(VecDeque::new())),
            trade_subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
            order_statuses,
        }
    }
}
#[async_trait]
impl ExchangeAccountInfo for BinanceLiveAccount {
    fn get_exchange_id(&self) -> ExchangeId {
        ExchangeId::Binance
    }
    async fn get_open_orders(&self, symbol: &Symbol) -> Arc<DashSet<Order>> {
            self.open_orders
                .get(symbol)
                .unwrap()
                .clone()
    }

    async fn get_symbol_account(&self, symbol: &Symbol) -> SymbolAccount {
        self.symbol_accounts
            .get(symbol)
            .unwrap()
            .clone()
    }

    async fn get_past_trades(&self, symbol: &Symbol, length: Option<usize>) -> Arc<DashSet<Trade>> {
        if let Some(length) = length {
            let mut trades_vec = self.trade_history.get(symbol).unwrap().iter().map(|v| v.clone()).collect::<Vec<_>>();
            trades_vec.sort_by(|a, b| b.time.cmp(&a.time));
            trades_vec.truncate(length);
            let trades = trades_vec.into_iter().collect::<DashSet<Trade>>();
            Arc::new(trades)
        } else {
                self.trade_history
                    .get(symbol)
                    .unwrap()
                    .clone()
        }
    }

    async fn get_position(&self, symbol: &Symbol) -> Arc<Position> {
        let account = self.account.read().await;
        if let Ok(position_info) = account.position_information(symbol.symbol.clone()).await {
            if position_info.len() == 0 {
                return Arc::new(Position::new(
                    Side::Ask,
                    symbol.clone(),
                    Decimal::ZERO,
                    Decimal::ZERO,
                ));
            } else {
                let pos = position_info.first().unwrap();
                let position = Position::new(
                    if pos.position_amount > 0.0 {
                        Side::Bid
                    } else {
                        Side::Ask
                    },
                    symbol.clone(),
                    Decimal::from_f64(pos.position_amount.abs()).unwrap(),
                    Decimal::from_f64(pos.entry_price).unwrap(),
                );
                return Arc::new(position);
            }
        } else {
            return Arc::new(Position::new(
                Side::Ask,
                symbol.clone(),
                Decimal::ZERO,
                Decimal::ZERO,
            ));
        }
    }

    async fn get_spread(&self, symbol: &Symbol) -> Arc<Spread> {
        let market = self.market.read().await;
        let price = market.get_price(symbol.symbol.clone()).await.unwrap();
        let mut spread = Spread::new(symbol.clone());
        spread.spread = Decimal::from_f64(price.price).unwrap();
        Arc::new(spread)
    }
    async fn get_order(&self, _symbol: &Symbol, _id: &Uuid) -> Vec<Order> {
        vec![]
    }
}

#[async_trait]
impl ExchangeAccount for BinanceLiveAccount {
    async fn limit_long(&self, order: Order) -> anyhow::Result<OrderStatus> {
        let account = self.account.read().await;
        let symbol = order.symbol.clone();
        match order.order_type {
            OrderType::Limit => {
                let res = account
                    .limit_buy(
                        symbol.symbol.clone(),
                        order.quantity.to_f64().unwrap(),
                        order.price.to_f64().unwrap(),
                        TimeInForce::GTC,
                    )
                    .await;
                if let Ok(_transaction) = res {
                    Ok(OrderStatus::Pending(order))
                } else {
                    Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
                }
            }
            OrderType::TakeProfit(_) => {
                let or = CustomOrderRequest {
                    symbol: symbol.symbol.clone(),
                    side: binance::account::OrderSide::Buy,
                    position_side: None,
                    order_type: binance::futures::account::OrderType::TakeProfitMarket,
                    time_in_force: Some(TimeInForce::GTC),
                    qty: Some(order.quantity.to_f64().unwrap()),
                    reduce_only: Some(true),
                    price: None,
                    stop_price: Some(order.price.to_f64().unwrap()),
                    close_position: None,
                    activation_price: None,
                    callback_rate: None,
                    working_type: None,
                    price_protect: None,
                };
                let res = account.custom_order(or).await;
                if let Ok(_transaction) = res {
                    Ok(OrderStatus::Pending(order))
                } else {
                    Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
                }
            }

            OrderType::StopLoss(_id) => {
                let res = account
                    .stop_market_close_buy(symbol.symbol.clone(), order.price.to_f64().unwrap())
                    .await;
                if let Ok(_transaction) = res {
                    Ok(OrderStatus::Pending(order))
                } else {
                    Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
                }
            }

            OrderType::StopLossTrailing(_id, distance) => {
                let trail_percent = (distance.to_f64().unwrap() * 100_f64)
                    / order.price.to_f64().unwrap().min(5.0).max(0.1);
                let or = CustomOrderRequest {
                    symbol: symbol.symbol.clone(),
                    side: binance::account::OrderSide::Buy,
                    position_side: None,
                    order_type: binance::futures::account::OrderType::TrailingStopMarket,
                    time_in_force: Some(TimeInForce::GTC),
                    qty: Some(order.quantity.to_f64().unwrap()),
                    reduce_only: Some(true),
                    price: Some(order.price.to_f64().unwrap()),
                    stop_price: None,
                    close_position: None,
                    activation_price: None,
                    callback_rate: Some(trail_percent),
                    working_type: None,
                    price_protect: None,
                };
                let res = account.custom_order(or).await;
                if let Ok(_transaction) = res {
                    Ok(OrderStatus::Pending(order))
                } else {
                    Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
                }
            }
            _ => {
                todo!()
            }
        }
    }

    async fn limit_short(&self, order: Order) -> anyhow::Result<OrderStatus> {
        let account = self.account.read().await;
        let symbol = order.symbol.clone();
        match order.order_type {
            OrderType::Limit => {
                let res = account
                    .limit_sell(
                        symbol.symbol.clone(),
                        order.quantity.to_f64().unwrap(),
                        order.price.to_f64().unwrap(),
                        TimeInForce::GTC,
                    )
                    .await;
                if let Ok(_transaction) = res {
                    Ok(OrderStatus::Pending(order))
                } else {
                    Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
                }
            }

            OrderType::TakeProfit(_) => {
                let or = CustomOrderRequest {
                    symbol: symbol.symbol.clone(),
                    side: binance::account::OrderSide::Sell,
                    position_side: None,
                    order_type: binance::futures::account::OrderType::TakeProfitMarket,
                    time_in_force: Some(TimeInForce::GTC),
                    qty: Some(order.quantity.to_f64().unwrap()),
                    reduce_only: Some(true),
                    price: None,
                    stop_price: Some(order.price.to_f64().unwrap()),
                    close_position: None,
                    activation_price: None,
                    callback_rate: None,
                    working_type: None,
                    price_protect: None,
                };
                let res = account.custom_order(or).await;
                if let Ok(_transaction) = res {
                    Ok(OrderStatus::Pending(order))
                } else {
                    Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
                }
            }

            OrderType::StopLoss(_id) => {
                let res = account
                    .stop_market_close_sell(symbol.symbol.clone(), order.price.to_f64().unwrap())
                    .await;
                if let Ok(_transaction) = res {
                    Ok(OrderStatus::Pending(order))
                } else {
                    Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
                }
            }

            OrderType::StopLossTrailing(_id, distance) => {
                let trail_percent = (distance.to_f64().unwrap() * 100_f64)
                    / order.price.to_f64().unwrap().min(5.0).max(0.1);
                let or = CustomOrderRequest {
                    symbol: symbol.symbol.clone(),
                    side: binance::account::OrderSide::Sell,
                    position_side: None,
                    order_type: binance::futures::account::OrderType::TrailingStopMarket,
                    time_in_force: Some(TimeInForce::GTC),
                    qty: Some(order.quantity.to_f64().unwrap()),
                    reduce_only: Some(true),
                    price: Some(order.price.to_f64().unwrap()),
                    stop_price: None,
                    close_position: None,
                    activation_price: None,
                    callback_rate: Some(trail_percent),
                    working_type: None,
                    price_protect: None,
                };
                let res = account.custom_order(or).await;
                if let Ok(_transaction) = res {
                    Ok(OrderStatus::Pending(order))
                } else {
                    Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
                }
            }
            _ => {
                todo!()
            }
        }
    }

    async fn market_long(&self, order: Order) -> anyhow::Result<OrderStatus> {
        let account = self.account.read().await;
        let symbol = order.symbol.clone();
        let res = account
            .market_buy(symbol.symbol.clone(), order.quantity.to_f64().unwrap())
            .await;
        if let Ok(transaction) = res {
            let mut or = order.clone();
            or.price = Decimal::from_f64(transaction.avg_price).unwrap();
            Ok(OrderStatus::Filled(order))
        } else {
            Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
        }
    }

    async fn market_short(&self, order: Order) -> anyhow::Result<OrderStatus> {
        let account = self.account.read().await;
        let symbol = order.symbol.clone();
        let res = account
            .market_sell(symbol.symbol.clone(), order.quantity.to_f64().unwrap())
            .await;
        if let Ok(transaction) = res {
            let mut or = order.clone();
            or.price = Decimal::from_f64(transaction.avg_price).unwrap();
            Ok(OrderStatus::Filled(order))
        } else {
            Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
        }
    }

    async fn cancel_order(&self, order: Order) -> anyhow::Result<OrderStatus> {
        Ok(OrderStatus::Canceled(order, "canceled".to_string()))
    }
}
#[async_trait]
impl EventSink<Order> for BinanceLiveExecutor {
    fn get_receiver(&self) -> Receiver<(Order, Option<Arc<Notify>>)> {
        self.orders.clone()
    }

    async fn handle_event(&self, event_msg: Order) -> anyhow::Result<()> {
        let fut = self.process_order(event_msg);
        let order_status_q = self.order_status_q.clone();
            if let Ok(res) = fut {
                order_status_q.write().await.push_back((res, None));
            }
            Ok(())
    }

}
#[async_trait]
impl EventEmitter<OrderStatus> for BinanceLiveExecutor {
    fn get_subscribers(&self) -> Arc<RwLock<Sender<(OrderStatus, Option<Arc<Notify>>)>>> {
        self.order_status_subscribers.clone()
    }

    async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
        let q = self.order_status_q.clone();
        let subs = self.order_status_subscribers.clone();
        Ok(tokio::spawn(async move {
            loop {
                let mut w = q.write().await;
                let order_status = w.pop_front();
                drop(w);
                if let Some(order_status) = order_status {
                    let subs = subs.read().await;
                    match subs.broadcast(order_status.clone()).await {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("error broadcasting order status: {:?}", e);
                        }
                    }
                }
            }
        }))
    }
}

#[async_trait]
impl EventSink<OrderStatus> for BinanceLiveAccount {
    fn get_receiver(&self) -> Receiver<(OrderStatus, Option<Arc<Notify>>)> {
        self.order_statuses.clone()
    }
    async fn handle_event(
        &self,
        event_msg: OrderStatus,
    ) -> anyhow::Result<()> {
        // let open_orders = self.open_orders.clone();
        // let order_history = self.order_history.clone();
        // let trade_q = self.trade_q.clone();
        // let all_positions = self.positions.clone();
        let notifier = self.notifier.clone();
            match &event_msg {
                OrderStatus::Pending(_) => {}
                OrderStatus::Filled(order) => {
                    if order.order_type == OrderType::Market {
                        // let browser = Browser::default().unwrap();
                        // let tab = browser.wait_for_initial_tab().unwrap();
                        // tab.navigate_to(&format!("https://www.binance.com/en/futures/{}", order.symbol.symbol)).unwrap();
                        // tab.wait_until_navigated().unwrap();
                        // tokio::time::sleep(Duration::from_secs(10));
                        //
                        // let png_data = tab.capture_screenshot(
                        // 	ScreenshotFormat::PNG,
                        // 	None,
                        // 	true).unwrap();
                        // let file = format!("{}-screenshot.png", order.id);
                        //
                        // std::fs::write(&file, png_data)?;
                        //

                        let notification = Notification {
                            message: order.to_markdown_message(),
                            attachment: None,
                        };

                        let n = notifier.read().await;
                        n.notify(notification).await;
                    }
                }
                OrderStatus::Canceled(order, reason) => {
                    let notification = Notification {
                        message: order.to_markdown_message() + "\n*Canceled*: " + reason,
                        attachment: None,
                    };
                    let n = notifier.read().await;
                    n.notify(notification).await;
                }

                _ => {}
            }
            Ok(())
    }

}
#[async_trait]
impl EventSink<TfTrades> for BinanceLiveAccount {
    fn get_receiver(&self) -> Receiver<(TfTrades,Option<Arc<Notify>>)> {
        self.tf_trades.clone()
    }
    async fn handle_event(&self, event_msg: TfTrades) -> anyhow::Result<()> {
        // let open_orders = self.open_orders.clone();
        //
        // let trade_q = self.trade_q.clone();
        // let filled_orders = self.order_history.clone();
        // let positions = self.positions.clone();

        // sync state
        Ok(())
    }


}

impl TradeExecutor for BinanceLiveExecutor {
    type Account = BinanceLiveAccount;
    fn get_account(&self) -> Arc<Self::Account> {
        self.account.clone()
    }
}
