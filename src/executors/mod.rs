#[allow(dead_code)]

pub(crate) mod live;
mod notification;
mod position;
pub mod simulated;

#[allow(dead_code)]
mod spread;
mod order_state_machine;

use crate::events::{EventEmitter, EventSink};
use crate::types::{ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, SymbolAccount, Trade};
use async_std::sync::Arc;
use async_trait::async_trait;
pub use position::*;
pub use spread::*;
use dashmap::DashSet;
use std::collections::HashSet;
use std::fmt::Debug;
use async_broadcast::Sender;
use tokio::sync::{Notify, RwLock};
use tracing::trace;

#[async_trait]
pub trait ExchangeAccountInfo: Send + Sync {
    #[allow(unused)]
    fn get_exchange_id(&self) -> ExchangeId;
    async fn get_open_orders(&self, symbol: &Symbol) -> Arc<HashSet<Order>>;
    async fn get_symbol_account(&self, symbol: &Symbol) -> SymbolAccount;

    #[cfg(test)]
    async fn get_past_trades(&self, symbol: &Symbol, length: Option<usize>) -> Arc<HashSet<Trade>>;
    async fn get_position(&self, symbol: &Symbol) -> Arc<Position>;
    async fn get_spread(&self, symbol: &Symbol) -> Arc<Spread>;
    #[cfg(test)]
    async fn get_order(&self, symbol: &Symbol, id: &uuid::Uuid) -> Vec<Order>;
}

#[async_trait]
pub trait ExchangeAccount: ExchangeAccountInfo {
    async fn limit_long(&self, order: Order) -> anyhow::Result<OrderStatus>;
    async fn limit_short(&self, order: Order) -> anyhow::Result<OrderStatus>;
    async fn market_long(&self, order: Order) -> anyhow::Result<OrderStatus>;
    async fn market_short(&self, order: Order) -> anyhow::Result<OrderStatus>;
    async fn cancel_order(&self, order: Order) -> anyhow::Result<OrderStatus>;
}

pub trait TradeExecutor: EventSink<Order> + EventEmitter<OrderStatus> {
    type Account: ExchangeAccount + 'static;
    fn get_account(&self) -> Arc<Self::Account>;

    fn process_order(&self, order: Order) -> anyhow::Result<OrderStatus> {
        let account = self.get_account();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            match order.order_type {
                OrderType::Limit
                | OrderType::TakeProfit(_)
                | OrderType::StopLoss(_)
                | OrderType::StopLossTrailing(_, _) => match order.side {
                    Side::Bid => rt.block_on(account.limit_long(order)),
                    Side::Ask => rt.block_on(account.limit_short(order)),
                },
                OrderType::Cancel(_) | OrderType::CancelFor(_) => {
                    rt.block_on(account.cancel_order(order))
                }
                OrderType::Market => match order.side {
                    Side::Bid => rt.block_on(account.market_long(order)),
                    Side::Ask => rt.block_on(account.market_short(order)),
                },
                _ => {
                    todo!()
                }
            }
        })
        .join()
        .unwrap()
    }
}
pub async fn broadcast_and_wait<T: Clone + Debug>(
    channel: &Arc<RwLock<Sender<(T, Option<Arc<Notify>>)>>>,
    value: T,
) {
    let notifier = Arc::new(Notify::new());
    let notified = notifier.notified();
    let channel = channel.read().await;
    if let Err(e) = channel.broadcast((value, Some(notifier.clone()))).await {
        trace!("Failed to broadcast notification: {}", e);
    }
    notified.await;
}

pub async fn broadcast<T: Clone + Debug>(
    channel: &Arc<RwLock<Sender<(T, Option<Arc<Notify>>)>>>,
    value: T,
) {
    let channel = channel.read().await;
    if let Err(e) = channel.broadcast((value, None)).await {
        trace!("Failed to broadcast notification: {}", e);
    }
}
