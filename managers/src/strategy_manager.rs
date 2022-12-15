use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;
use anyhow::Error;
use async_std::sync::Arc;
use futures::stream::FuturesUnordered;
use async_broadcast::{Receiver, Sender};
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::StreamExt;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use binance_q_events::{EventEmitter, EventSink};
use binance_q_types::{ExecutionCommand, GlobalConfig, StrategyEdge, TfTrades};


use tonic::{transport::Server, Request, Response, Status};
use signals::signal_generator_server::{SignalGenerator, SignalGeneratorServer};
use signals::{NotifySignalResponse, Signal};

pub mod signals {
	tonic::include_proto!("signals");
}

#[derive(Debug, Clone)]
pub struct SignalGeneratorService {

}

#[tonic::async_trait]
impl SignalGenerator for SignalGeneratorService {
	async fn notify_signal(&self, request: Request<Signal>) -> Result<Response<NotifySignalResponse>, Status> {
		println!("Got a request: {:?}", request);
		
		
		
		Ok(Response::new(NotifySignalResponse { confirmed: true}))
	}
}
#[derive(Clone)]
pub struct StrategyManager {
	global_config: GlobalConfig,
	pub command_subscribers: Arc<RwLock<Sender<ExecutionCommand>>>,
	tf_trades: Arc<RwLock<Receiver<TfTrades>>>,
	tf_trades_working: Arc<std::sync::RwLock<bool>>,
	command_q: Arc<RwLock<VecDeque<ExecutionCommand>>>
}

#[async_trait]
impl EventEmitter<ExecutionCommand> for StrategyManager {
	fn get_subscribers(&self) -> Arc<RwLock<Sender<ExecutionCommand>>> {
		self.command_subscribers.clone()
	}
	async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
		let q = self.command_q.clone();
		let subs = self.command_subscribers.clone();
		Ok(tokio::spawn(async move {
			loop {
				let mut w = q.write().await;
				let first = w.pop_front();
				drop(w);
				if let Some(first) = first {
					let mut ws = subs.write().await;
					ws.broadcast(first).await.unwrap();
				}
			}
		}))
	}
}

impl EventSink<TfTrades> for StrategyManager {
	fn get_receiver(&self) -> Arc<RwLock<Receiver<TfTrades>>> {
		self.tf_trades.clone()
	}
	fn working(&self) -> bool {
		self.tf_trades_working.read().unwrap().clone()
	}
	fn set_working(&self, working: bool) -> anyhow::Result<()> {
		*self.tf_trades_working.write().unwrap() = working;
		Ok(())
	}
	fn handle_event(&self, event_msg: TfTrades) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
		Ok(tokio::spawn(async move {
			Ok(())
		}))
	}
}

impl StrategyManager {
	pub fn new(global_config: GlobalConfig, tf_trades: Receiver<TfTrades>) -> Self {
		
		Self {
			global_config,
			command_subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
			tf_trades: Arc::new(RwLock::new(tf_trades)),
			tf_trades_working: Arc::new(std::sync::RwLock::new(false)),
			command_q: Arc::new(RwLock::new(VecDeque::new()))
		}
	}
	
	

}