use std::collections::VecDeque;
use std::future::Future;
use std::path::Path;
use std::time::Duration;
use anyhow::Error;
use async_std::sync::Arc;
use futures::stream::FuturesUnordered;
use async_broadcast::{Receiver, Sender};
use async_std::io::ReadExt;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::StreamExt;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use binance_q_events::{EventEmitter, EventSink};
use binance_q_types::{ExchangeId, ExecutionCommand, GlobalConfig, Kline, StrategyEdge, Symbol, TfTrades};
use tokio::net::{UnixStream, UnixListener};
use tokio::select;
use serde::{Serialize, Deserialize};
use serde_pickle::{DeOptions, SerOptions};
use tokio::io::{AsyncWriteExt, AsyncReadExt};

use tonic::{transport::Server, Request, Response, Status};
use signals::signal_generator_server::{SignalGenerator, SignalGeneratorServer};
use signals::{NotifySignalResponse, Signal, Side};

pub mod signals {
	tonic::include_proto!("signals");
}

#[derive(Debug, Clone)]
pub struct SignalGeneratorService {
	pub signals_q: Arc<RwLock<VecDeque<ExecutionCommand>>>
}

#[tonic::async_trait]
impl SignalGenerator for SignalGeneratorService {
	async fn notify_signal(&self, request: Request<Signal>) -> Result<Response<NotifySignalResponse>, Status> {
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
			1 => {
				w.push_back(ExecutionCommand::OpenLongPosition(symbol, msg.probability as f64))
			},
			2 => {
				w.push_back(ExecutionCommand::OpenShortPosition(symbol, msg.probability as f64))
			}
			_ => {}
		}
		
		Ok(Response::new(NotifySignalResponse { confirmed: true}))
	}
}
#[derive(Clone)]
pub struct StrategyManager {
	global_config: GlobalConfig,
	pub command_subscribers: Arc<RwLock<Sender<ExecutionCommand>>>,
	signal_generators: Arc<RwLock<SignalGeneratorService>>,
	klines: Arc<RwLock<Receiver<Kline>>>,
	klines_working: Arc<std::sync::RwLock<bool>>,
	command_q: Arc<RwLock<VecDeque<ExecutionCommand>>>,
	backtest_sock: Arc<RwLock<UnixStream>>
}

#[async_trait]
impl EventEmitter<ExecutionCommand> for StrategyManager {
	fn get_subscribers(&self) -> Arc<RwLock<Sender<ExecutionCommand>>> {
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
					ws.broadcast(first).await.unwrap();
				}
			}
		}))
	}
}

#[derive(Serialize, Deserialize)]
struct SocketMessage {
	pub msg: String
}

impl EventSink<Kline> for StrategyManager {
	fn get_receiver(&self) -> Arc<RwLock<Receiver<Kline>>> {
		self.klines.clone()
	}
	fn working(&self) -> bool {
		self.klines_working.read().unwrap().clone()
	}
	fn set_working(&self, working: bool) -> anyhow::Result<()> {
		*self.klines_working.write().unwrap() = working;
		Ok(())
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
				},
				Err(e) => {
					Err(Error::new(e))
				}
			}
		}))
	}
}

impl StrategyManager {
	pub async fn new(global_config: GlobalConfig, klines: Receiver<Kline>) -> Self {
		let addr = "[::1]:50051".parse().unwrap();
		let service = SignalGeneratorService {
			signals_q: Arc::new(RwLock::new(VecDeque::new()))
		};
		let signal_server = SignalGeneratorServer::new(service.clone());
		
		tokio::spawn(async move  {
			Server::builder()
				  .add_service(signal_server)
				  .serve(addr)
				  .await
				  .unwrap();
		});
		
		Self {
			global_config,
			command_subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
			klines: Arc::new(RwLock::new(klines)),
			signal_generators: Arc::new(RwLock::new(service)),
			klines_working: Arc::new(std::sync::RwLock::new(false)),
			command_q: Arc::new(RwLock::new(VecDeque::new())),
			backtest_sock: Arc::new(RwLock::new(UnixStream::connect("/tmp/backtest.sock").await.unwrap()))
		}
		
		
		
	}
	
	

}