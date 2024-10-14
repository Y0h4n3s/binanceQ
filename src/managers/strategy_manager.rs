use crate::events::{EventEmitter, EventSink};
use crate::executors::ExchangeAccount;
use crate::types::{
    ClosePolicy, ExchangeId, ExecutionCommand, FromProtoOrderType, GlobalConfig, Kline, Mode,
    Order, OrderStatus, OrderType, Symbol,
};
use anyhow::Error;
use async_broadcast::{Receiver, Sender};
use async_std::io::ReadExt;
use async_std::sync::Arc;
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_pickle::DeOptions;
use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::select;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;

use rust_decimal::prelude::*;
use signals::signal_generator_server::{SignalGenerator, SignalGeneratorServer};
use signals::{
    NotifyResponse, NotifySignalResponse, Order as POrder, Orders as POrders,
    Position as PPosition, Signal, Spread as PSpread, Symbol as PSymbol,
};
use tonic::{transport::Server, Request, Response, Status};

pub mod signals {
    tonic::include_proto!("signals");
}

#[derive(Clone)]
pub struct SignalGeneratorService {
    pub signals_q: Arc<RwLock<VecDeque<ExecutionCommand>>>,
    pub account: Box<Arc<dyn ExchangeAccount>>,
}

#[tonic::async_trait]
impl SignalGenerator for SignalGeneratorService {
    async fn notify_signal(
        &self,
        request: Request<Signal>,
    ) -> Result<Response<NotifySignalResponse>, Status> {
        let msg = request.into_inner();
        let sym = msg.symbol.unwrap();
        let symbol = Symbol {
            symbol: sym.symbol.clone(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: sym.base_asset_precision,
            quote_asset_precision: sym.quote_asset_precision,
        };

        let mut w = self.signals_q.write().await;
        match msg.side {
            1 => w.push_back(ExecutionCommand::OpenLongPosition(
                symbol,
                msg.probability as f64,
            )),
            2 => w.push_back(ExecutionCommand::OpenShortPosition(
                symbol,
                msg.probability as f64,
            )),
            _ => {}
        }

        Ok(Response::new(NotifySignalResponse { confirmed: true }))
    }

    async fn notify_orders(
        &self,
        request: Request<POrders>,
    ) -> Result<Response<NotifyResponse>, Status> {
        let msg = request.into_inner();
        let orders = msg.orders;
        let mut w = self.signals_q.write().await;
        for o in orders {
            if let Some(symbol) = o.symbol {
                let trailing = if o.order_type == 5 {
                    Decimal::from_str_exact(&o.extra).unwrap()
                } else {
                    Default::default()
                };
                let proto_order_type = FromProtoOrderType {
                    uuid: uuid::Uuid::parse_str(&o.for_id).unwrap(),
                    my_type: o.order_type,
                    trailing,
                };
                let symbol = Symbol {
                    symbol: symbol.symbol.clone(),
                    exchange: ExchangeId::Simulated,
                    base_asset_precision: symbol.base_asset_precision,
                    quote_asset_precision: symbol.quote_asset_precision,
                };
                let order = Order {
                    id: uuid::Uuid::parse_str(&o.id).unwrap(),
                    symbol: symbol.clone(),
                    side: if o.side == 1 {
                        crate::types::Side::Bid
                    } else {
                        crate::types::Side::Ask
                    },
                    price: Decimal::from_f64(o.price).unwrap(),
                    quantity: Decimal::from_f64(o.quantity).unwrap(),
                    time: o.timestamp,
                    order_type: OrderType::from(proto_order_type),
                    lifetime: o.lifetime,
                    close_policy: ClosePolicy::from(o.close_policy),
                };
                w.push_back(ExecutionCommand::ExecuteOrder(order));
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(Response::new(NotifyResponse {
            success: true,
            message: "ok".to_string(),
        }))
    }

    async fn get_position(&self, request: Request<PSymbol>) -> Result<Response<PPosition>, Status> {
        let msg = request.into_inner();
        let sym = Symbol {
            symbol: msg.symbol.clone(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: msg.base_asset_precision,
            quote_asset_precision: msg.quote_asset_precision,
        };

        let account = self.account.clone();
        let position = account.get_position(&sym).await;

        let p_position = PPosition {
            id: position.trade_id,
            symbol: Some(msg),
            side: if position.side == crate::types::Side::Bid {
                1
            } else {
                2
            },
            qty: position.qty.to_f64().unwrap(),
            quote_qty: position.quote_qty.to_f64().unwrap(),
            avg_price: position.avg_price.to_f64().unwrap(),
            extra: "".to_string(),
        };
        Ok(Response::new(p_position))
    }

    async fn get_spread(&self, request: Request<PSymbol>) -> Result<Response<PSpread>, Status> {
        let msg = request.into_inner();
        let sym = Symbol {
            symbol: msg.symbol.clone(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: msg.base_asset_precision,
            quote_asset_precision: msg.quote_asset_precision,
        };

        let account = self.account.clone();
        let spread = account.get_spread(&sym).await;

        let p_spread = PSpread {
            symbol: Some(msg),
            spread: spread.spread.to_f64().unwrap(),
            time: spread.time,
        };
        Ok(Response::new(p_spread))
    }

    async fn get_open_orders(
        &self,
        request: Request<PSymbol>,
    ) -> Result<Response<POrders>, Status> {
        let msg = request.into_inner();
        let sym = Symbol {
            symbol: msg.symbol.clone(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: msg.base_asset_precision,
            quote_asset_precision: msg.quote_asset_precision,
        };

        let account = self.account.clone();
        let orders = account.get_open_orders(&sym).await;

        let mut p_orders = vec![];
        for order in orders.iter() {
            if let OrderStatus::Pending(order) = order {
                let mut for_id = "".to_string();
                match order.order_type {
                    OrderType::StopLoss(id)
                    | OrderType::TakeProfit(id)
                    | OrderType::CancelFor(id)
                    | OrderType::StopLossTrailing(id, _) => {
                        for_id = id.to_string();
                    }

                    _ => {}
                }
                let p_order = POrder {
                    id: order.id.to_string(),
                    for_id,
                    symbol: Some(msg.clone()),
                    side: if order.side == crate::types::Side::Bid {
                        1
                    } else {
                        2
                    },
                    price: order.price.to_f64().unwrap(),
                    quantity: order.quantity.to_f64().unwrap(),
                    timestamp: order.time,
                    order_type: 1,
                    lifetime: order.lifetime,
                    close_policy: 1,
                    extra: "".to_string(),
                };
                p_orders.push(p_order);
            }
        }
        Ok(Response::new(POrders { orders: p_orders }))
    }
}
#[derive(Clone)]
pub struct StrategyManager {
    global_config: GlobalConfig,
    pub command_subscribers: Arc<RwLock<Sender<(ExecutionCommand, Option<Arc<Notify>>)>>>,
    signal_generators: Arc<RwLock<SignalGeneratorService>>,
    klines: Arc<RwLock<Receiver<(Kline, Option<Arc<Notify>>)>>>,
    klines_working: Arc<std::sync::RwLock<bool>>,
    command_q: Arc<RwLock<VecDeque<ExecutionCommand>>>,
    backtest_sock: Arc<RwLock<UnixStream>>,
}

#[async_trait]
impl EventEmitter<ExecutionCommand> for StrategyManager {
    fn get_subscribers(&self) -> Arc<RwLock<Sender<(ExecutionCommand, Option<Arc<Notify>>)>>> {
        self.command_subscribers.clone()
    }
    async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
        let signal_service = self.signal_generators.read().await;
        let q = signal_service.signals_q.clone();
        drop(signal_service);
        let subs = self.command_subscribers.clone();
        Ok(tokio::spawn(async move {
            loop {
                let mut w = q.write().await;
                let first = w.pop_front();
                drop(w);
                if let Some(first) = first {
                    let mut ws = subs.write().await;
                    ws.broadcast((first, None)).await.unwrap();
                }
            }
        }))
    }
}

#[derive(Serialize, Deserialize)]
struct SocketMessage {
    pub msg: String,
}

impl EventSink<Kline> for StrategyManager {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<(Kline, Option<Arc<Notify>>)>>> {
        self.klines.clone()
    }
    fn handle_event(&self, event_msg: Kline) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        let sock = self.backtest_sock.clone();
        Ok(tokio::spawn(async move {
            // TODO: make sure socket is open

            // this is only run during backtest mode,
            // send the kline over to python via socket and wait for any signals
            let mut socket = sock.write().await;
            let mut serialized = vec![];
            serde_pickle::to_writer(&mut serialized, &event_msg, Default::default())?;
            match socket.write(serialized.as_slice()).await {
                Ok(_) => {
                    // wait for confirmation
                    // stop after 2 seconds if no confirmation
                    select! {
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {
                            Ok(())
                        }
                        _ = socket.readable() => {
                            let mut buf = [0; 1024];
                            let n = socket.read(&mut buf).await?;
                            let response = serde_pickle::from_slice::<SocketMessage>(&buf[..n], DeOptions::default())?;
                            if &response.msg == "OK" {
                                Ok(())
                            } else {
                                Err(Error::msg("Invalid confirmation received from backtest"))
                            }
                        }
                    }
                }
                Err(e) => Err(Error::new(e)),
            }
        }))
    }
    fn working(&self) -> bool {
        *self.klines_working.read().unwrap()
    }
    fn set_working(&self, working: bool) -> anyhow::Result<()> {
        *self.klines_working.write().unwrap() = working;
        Ok(())
    }
}

pub struct StrategyManagerConfig {
    pub symbol: Symbol,
}

impl StrategyManager {
    pub async fn new(
        config: StrategyManagerConfig,
        global_config: GlobalConfig,
        klines: Receiver<(Kline, Option<Arc<Notify>>)>,
        grpc_server_port: String,
        account: Box<Arc<dyn ExchangeAccount>>,
    ) -> Self {
        let addr = format!("0.0.0.0:{}", grpc_server_port).parse().unwrap();
        let service = SignalGeneratorService {
            signals_q: Arc::new(RwLock::new(VecDeque::new())),
            account,
        };
        let signal_server = SignalGeneratorServer::new(service.clone());

        tokio::spawn(async move {
            Server::builder()
                .add_service(signal_server)
                .serve(addr)
                .await
                .unwrap();
        });
        let sock_addr = if global_config.mode == Mode::Backtest {
            format!("/tmp/backtest-{}.sock", config.symbol.symbol.clone())
        } else {
            format!("/tmp/live-{}.sock", config.symbol.symbol.clone())
        };

        Self {
            global_config,
            command_subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
            klines: Arc::new(RwLock::new(klines)),
            signal_generators: Arc::new(RwLock::new(service)),
            klines_working: Arc::new(std::sync::RwLock::new(false)),
            command_q: Arc::new(RwLock::new(VecDeque::new())),
            backtest_sock: Arc::new(RwLock::new(UnixStream::connect(sock_addr).await.unwrap())),
        }
    }
}
