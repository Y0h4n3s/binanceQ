use std::future::Future;
use std::time::Duration;
use async_std::sync::Arc;
use futures::stream::FuturesUnordered;
use kanal::{AsyncReceiver, AsyncSender};
use crate::events::EventEmitter;
use crate::GlobalConfig;
use crate::managers::risk_manager::ExecutionCommand;
use crate::strategies::{SignalGenerator, StrategyEdge};
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::StreamExt;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;


pub struct StrategyManager {
	global_config: GlobalConfig,
	pub open_long_strategies: Arc<RwLock<Vec<Box<dyn SignalGenerator>>>>,
	pub open_short_strategies: Arc<RwLock<Vec<Box<dyn SignalGenerator>>>>,
	pub close_long_strategies: Arc<RwLock<Vec<Box<dyn SignalGenerator>>>>,
	pub close_short_strategies: Arc<RwLock<Vec<Box<dyn SignalGenerator>>>>,
	pub command_subscribers: Arc<RwLock<Vec<AsyncSender<ExecutionCommand>>>>,

}




#[async_trait]
impl EventEmitter<'_, ExecutionCommand> for StrategyManager {
	fn get_subscribers(&self) -> Arc<RwLock<Vec<AsyncSender<ExecutionCommand>>>> {
		self.command_subscribers.clone()
	}
	
	async fn emit(&self) -> anyhow::Result<JoinHandle<()>>{
		let mut futures:Vec<BoxFuture<'_, ()>> = vec![];
		let oss = self.open_short_strategies.read().await.iter().map(|s| dyn_clone::clone_box(&**s)).collect::<Vec<_>>();
		let ols = self.open_long_strategies.read().await.iter().map(|s| dyn_clone::clone_box(&**s)).collect::<Vec<_>>();
		let cls = self.close_long_strategies.read().await.iter().map(|s| dyn_clone::clone_box(&**s)).collect::<Vec<_>>();
		let css = self.close_short_strategies.read().await.iter().map(|s| dyn_clone::clone_box(&**s)).collect::<Vec<_>>();
		let subscriber = self.command_subscribers.read().await.clone().first().unwrap().clone();
		futures.push(Box::pin(StrategyManager::emit_open_short_signals(oss, subscriber.clone())));
		futures.push(Box::pin(StrategyManager::emit_open_long_signals(ols, subscriber.clone())));
		futures.push(Box::pin(StrategyManager::emit_close_long_signals(cls, subscriber.clone())));
		futures.push(Box::pin(StrategyManager::emit_close_short_signals(css, subscriber.clone())));
		Ok(tokio::spawn(async move {
			futures::future::join_all(futures).await;
		}))
	}
}

impl StrategyManager {
	pub fn new(global_config: GlobalConfig) -> Self {
		Self {
			global_config,
			open_long_strategies: Arc::new(RwLock::new(vec![])),
			open_short_strategies: Arc::new(RwLock::new(vec![])),
			close_long_strategies: Arc::new(RwLock::new(vec![])),
			close_short_strategies: Arc::new(RwLock::new(vec![])),
			command_subscribers: Arc::new(RwLock::new(Vec::new())),

		}
	}
	
	
	
	async fn emit_open_short_signals( oss: Vec<Box<dyn SignalGenerator>>, subscriber: AsyncSender<ExecutionCommand>){
		for s in oss.iter() {
			let subscriber = subscriber.clone();
			let s1 = dyn_clone::clone_box(&**s);
			tokio::spawn(async move {
				loop {
					let signal = s1.get_signal().await;
					if signal == StrategyEdge::Short {
							subscriber.send(ExecutionCommand::OpenShortPosition).await.unwrap()
					}
					tokio::time::sleep(Duration::from_secs(1)).await;
				}
			});
		}
	}
	
	async fn emit_open_long_signals( ols: Vec<Box<dyn SignalGenerator>>, subscriber: AsyncSender<ExecutionCommand>){
		for s in ols.iter() {
			let subscriber = subscriber.clone();
			let s1 = dyn_clone::clone_box(&**s);
			tokio::spawn(async move {
				loop {
					let signal = s1.get_signal().await;
					if signal == StrategyEdge::Long {
							subscriber.send(ExecutionCommand::OpenLongPosition).await.unwrap()
					}
					tokio::time::sleep(Duration::from_secs(1)).await;
				}
			});
		}
	}
	
	async fn emit_close_long_signals( cls: Vec<Box<dyn SignalGenerator>>, subscriber: AsyncSender<ExecutionCommand>){
		for s in cls.iter() {
			let subscriber = subscriber.clone();
			let s1 = dyn_clone::clone_box(&**s);
			tokio::spawn(async move {
				loop {
					let signal = s1.get_signal().await;
					if signal == StrategyEdge::CloseLong {
							subscriber.send(ExecutionCommand::CloseLongPosition).await.unwrap()
					}
					tokio::time::sleep(Duration::from_secs(1)).await;
				}
			});
		}
	}
	
	async fn emit_close_short_signals( css: Vec<Box<dyn SignalGenerator>>, subscriber: AsyncSender<ExecutionCommand>){
		for s in css.iter() {
			let subscriber = subscriber.clone();
			let s1 = dyn_clone::clone_box(&**s);
			tokio::spawn(async move {
				loop {
					let signal = s1.get_signal().await;
					if signal == StrategyEdge::CloseShort {
							subscriber.send(ExecutionCommand::CloseShortPosition).await.unwrap()
					}
					tokio::time::sleep(Duration::from_secs(1)).await;
				}
			});
		}
	}
	
	pub async fn with_long_entry_strategy<S: SignalGenerator + Clone + Send + Sync + 'static>(mut self, strategy: S) -> Self {
		self.open_long_strategies.write().await.push(Box::new(strategy));
		self
	}
	
	pub async fn with_short_entry_strategy<S: SignalGenerator + Clone + Send + Sync + 'static> (mut self, strategy: S) -> Self {
		self.open_short_strategies.write().await.push(Box::new(strategy));
		self
	}
	
	pub async fn with_long_exit_strategy<S: SignalGenerator + Clone + Send + Sync + 'static> (mut self, strategy: S) -> Self {
		self.close_long_strategies.write().await.push(Box::new(strategy));
		self
	}
	
	pub async fn with_short_exit_strategy<S: SignalGenerator + Clone + Send + Sync + 'static> (mut self, strategy: S) -> Self {
		self.close_short_strategies.write().await.push(Box::new(strategy));
		self
	}
	
	pub async fn with_entry_strategy<S: SignalGenerator + Clone + Send + Sync + 'static> (mut self, strategy: S) -> Self {
		self.open_long_strategies.write().await.push(Box::new(strategy.clone()));
		self.open_short_strategies.write().await.push(Box::new(strategy));
		self
	}
	
	pub async fn with_exit_strategy<S: SignalGenerator + Clone + Send + Sync + 'static> (mut self, strategy: S) -> Self {
		self.close_long_strategies.write().await.push(Box::new(strategy.clone()));
		self.close_short_strategies.write().await.push(Box::new(strategy));
		self
	}
	pub async fn with_strategy<S: SignalGenerator + Clone + Send + Send + Sync + 'static> (mut self, strategy: S) -> Self {
		self.open_long_strategies.write().await.push(Box::new(strategy.clone()));
		self.open_short_strategies.write().await.push(Box::new(strategy.clone()));
		self.close_long_strategies.write().await.push(Box::new(strategy.clone()));
		self.close_short_strategies.write().await.push(Box::new(strategy));
		self
	}
}