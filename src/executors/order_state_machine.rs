/// This module defines the `OrderStateMachine` struct, which manages the state of an order.
/// It processes different types of orders, such as market, limit, stop-loss, and take-profit orders.

use crate::types::{Order, OrderStatus, OrderType, Side, Symbol, Trade, TradeEntry};
use crate::executors::{broadcast, broadcast_and_wait, Position};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_broadcast::Sender;
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::debug;
use uuid::Uuid;

pub struct OrderStateMachine<'a> {
    /// Manages the state of an order and processes it based on its type.
    /// Updates positions, open orders, and notifies subscribers of order status changes.
    order: &'a mut Order,
    index: usize,
    positions: &'a Arc<Mutex<HashMap<Symbol, Position>>>,
    open_orders: &'a mut Vec<Order>,
    trade_subscribers: &'a Arc<RwLock<Sender<(Trade, Option<Arc<Notify>>)>>>,
    order_status_subscribers: &'a Arc<RwLock<Sender<(OrderStatus, Option<Arc<Notify>>)>>>,
}

impl<'a> OrderStateMachine<'a> {
    /// Creates a new `OrderStateMachine` for the given order and account state.
    /// Initializes the state machine with references to positions, open orders, and subscribers.
    pub fn new(
        order: &'a mut Order,
        index: usize,
        positions: &'a Arc<Mutex<HashMap<Symbol, Position>>>,
        open_orders: &'a mut Vec<Order>,
        trade_subscribers: &'a Arc<RwLock<Sender<(Trade, Option<Arc<Notify>>)>>>,
        order_status_subscribers: &'a Arc<RwLock<Sender<(OrderStatus, Option<Arc<Notify>>)>>>,
    ) -> Self {
        Self {
            order,
            index,
            positions,
            open_orders,
            trade_subscribers,
            order_status_subscribers,
        }
    }

    /// Processes the order based on its type and the given trade entry.
    /// Updates the order state and notifies subscribers of any changes.
    pub async fn process_order(&mut self, trade: &TradeEntry) {
        // Prioritize market orders
        if self.order.order_type == OrderType::Market {
            self.process_market_order(trade).await;
        } else {
            match self.order.order_type {
                OrderType::Cancel(_) => self.process_cancel_order().await,
                OrderType::Limit => self.process_limit_order(trade).await,
                OrderType::StopLossLimit => self.process_stop_loss_limit_order(trade).await,
                OrderType::TakeProfitLimit => self.process_take_profit_limit_order(trade).await,
                _ => {}
            }
        }
    }

    async fn process_cancel_order(&mut self) {
        self.open_orders.remove(self.index);
        broadcast_and_wait(
            &self.order_status_subscribers,
            OrderStatus::Canceled(self.order.clone(), "Cancel Order".to_string()),
        )
        .await;
    }

    async fn process_market_order(&mut self, trade: &TradeEntry) {
        let mut lock = self.positions.lock().await;
        let mut position = lock.get_mut(&self.order.symbol).unwrap();
        if let Some(trade) = position.apply_order(self.order, trade.timestamp) {
            broadcast(&self.trade_subscribers, trade).await;
        }
        drop(lock);
        self.open_orders.remove(self.index);
        broadcast_and_wait(
            &self.order_status_subscribers,
            OrderStatus::Filled(self.order.clone()),
        )
        .await;
    }

    async fn process_limit_order(&mut self, trade: &TradeEntry) {
        let mut lock = self.positions.lock().await;
        let mut position = lock.get_mut(&self.order.symbol).unwrap();
        if self.order.side == Side::Bid {
            if trade.price.le(&self.order.price) {
                if let Some(trade) = position.apply_order(self.order, trade.timestamp) {
                    broadcast(&self.trade_subscribers, trade).await;
                }
                drop(lock);
                self.open_orders.remove(self.index);
                if trade.qty.lt(&self.order.quantity) {
                    broadcast_and_wait(
                        &self.order_status_subscribers,
                        OrderStatus::PartiallyFilled(self.order.clone(), trade.qty),
                    )
                    .await;
                } else {
                    broadcast_and_wait(
                        &self.order_status_subscribers,
                        OrderStatus::Filled(self.order.clone()),
                    )
                    .await;
                }
            }
        } else {
            if trade.price.ge(&self.order.price) {
                if let Some(trade) = position.apply_order(self.order, trade.timestamp) {
                    broadcast(&self.trade_subscribers, trade).await;
                }
                drop(lock);
                self.open_orders.remove(self.index);
                if trade.qty.lt(&self.order.quantity) {
                    broadcast_and_wait(
                        &self.order_status_subscribers,
                        OrderStatus::PartiallyFilled(self.order.clone(), trade.qty),
                    )
                    .await;
                } else {
                    broadcast_and_wait(
                        &self.order_status_subscribers,
                        OrderStatus::Filled(self.order.clone()),
                    )
                    .await;
                }
            }
        }
    }

    async fn process_stop_loss_limit_order(&mut self, trade: &TradeEntry) {
        let mut lock = self.positions.lock().await;
        let mut position = lock.get_mut(&self.order.symbol).unwrap();
        if !position.is_open() {
            self.open_orders.remove(self.index);
            broadcast_and_wait(
                &self.order_status_subscribers,
                OrderStatus::Canceled(self.order.clone(), "Stoploss on neutral position".to_string()),
            )
            .await;
            return;
        }
        if position.is_long() && trade.price.le(&self.order.price)
            || !position.is_long() && trade.price.ge(&self.order.price)
        {
            self.order.quantity = self.order.quantity.min(position.qty);
            if let Some(trade) = position.apply_order(self.order, trade.timestamp) {
                self.open_orders.remove(self.index);
                if trade.qty.lt(&self.order.quantity) {
                    broadcast_and_wait(
                        &self.order_status_subscribers,
                        OrderStatus::PartiallyFilled(self.order.clone(), trade.qty),
                    )
                    .await;
                } else {
                    broadcast_and_wait(
                        &self.order_status_subscribers,
                        OrderStatus::Filled(self.order.clone()),
                    )
                    .await;
                }
                let lock = self.trade_subscribers.read().await;
                drop(lock);
                broadcast(&self.trade_subscribers, trade).await;

            }
        }
    }

    async fn process_take_profit_limit_order(&mut self, trade: &TradeEntry) {
        let mut lock = self.positions.lock().await;
        let mut position = lock.get_mut(&self.order.symbol).unwrap();
        if !position.is_open() {
            self.open_orders.remove(self.index);
            broadcast_and_wait(
                &self.order_status_subscribers,
                OrderStatus::Canceled(self.order.clone(), "Take profit on neutral position".to_string()),
            )
            .await;
            return;
        }
        if position.is_long() && trade.price.ge(&self.order.price)
            || !position.is_long() && trade.price.le(&self.order.price)
        {
            self.order.quantity = self.order.quantity.min(position.qty);
            if let Some(trade) = position.apply_order(self.order, trade.timestamp) {
                self.open_orders.remove(self.index);
                if trade.qty.lt(&self.order.quantity) {
                    broadcast_and_wait(
                        &self.order_status_subscribers,
                        OrderStatus::PartiallyFilled(self.order.clone(), trade.qty),
                    )
                    .await;
                } else {
                    broadcast_and_wait(
                        &self.order_status_subscribers,
                        OrderStatus::Filled(self.order.clone()),
                    )
                    .await;
                }
                broadcast(&self.trade_subscribers, trade).await;
            }
        }
    }
}
