use futures::stream::FuturesUnordered;
use kanal::{AsyncReceiver, AsyncSender};
use crate::events::EventEmitter;
use crate::GlobalConfig;
use crate::managers::risk_manager::ExecutionCommand;
use crate::strategies::{SignalGenerator, StrategyEdge};
use async_trait::async_trait;
use futures::StreamExt;


pub struct StrategyManager {
	global_config: GlobalConfig,
	pub open_long_strategies: Vec<Box<dyn EventEmitter<StrategyEdge> >>,
	pub open_short_strategies: Vec<Box<dyn EventEmitter<StrategyEdge>>>,
	pub close_long_strategies: Vec<Box<dyn EventEmitter<StrategyEdge>>>,
	pub close_short_strategies: Vec<Box<dyn EventEmitter<StrategyEdge> >>,
	pub command_subscribers: Vec<AsyncSender<ExecutionCommand>>,
	open_long_signals: (AsyncSender<StrategyEdge>, AsyncReceiver<StrategyEdge>),
	open_short_signals: (AsyncSender<StrategyEdge>, AsyncReceiver<StrategyEdge>),
	close_long_signals: (AsyncSender<StrategyEdge>, AsyncReceiver<StrategyEdge>),
	close_short_signals: (AsyncSender<StrategyEdge>, AsyncReceiver<StrategyEdge>)
}



#[async_trait]
impl EventEmitter<ExecutionCommand> for StrategyManager {
	fn subscribe(&mut self, sender: AsyncSender<ExecutionCommand>) {
		self.command_subscribers.push(sender);
	}
	
	async fn emit(&self) {
		let command_subscribers = self.command_subscribers.clone();
		let mut futures = FuturesUnordered::new();
		for mut s in self.open_long_strategies {
			s.subscribe(self.open_long_signals.0.clone());
			let r = &self.open_long_signals.1;
			futures.push(s.emit());
			futures.push(Box::pin(StrategyManager::recv_commands(r, command_subscribers.first().unwrap().clone(), ExecutionCommand::OpenLongPosition)));
		}
		for mut s in self.open_short_strategies {
			s.subscribe(self.open_short_signals.0.clone());
			let r = &self.open_short_signals.1;
			futures.push(s.emit());
			futures.push(Box::pin(StrategyManager::recv_commands(r, command_subscribers.first().unwrap().clone(), ExecutionCommand::OpenShortPosition)));
			
		}
		for mut s in self.close_long_strategies {
			s.subscribe(self.close_long_signals.0.clone());
			futures.push(s.emit());
			let r = &self.close_long_signals.1;
			futures.push(Box::pin(StrategyManager::recv_commands(r, command_subscribers.first().unwrap().clone(), ExecutionCommand::CloseLongPosition)));
			
		}
		for mut s in self.close_short_strategies {
			s.subscribe(self.close_short_signals.0.clone());
			let r = &self.close_short_signals.1;
			futures.push(s.emit());
			futures.push(Box::pin(StrategyManager::recv_commands(r, command_subscribers.first().unwrap().clone(), ExecutionCommand::CloseShortPosition)));
			
		}
		while let done = futures.next() {
		}
	}
}

impl StrategyManager {
	pub fn new(global_config: GlobalConfig) -> Self {
		Self {
			global_config,
			open_long_strategies: vec![],
			open_short_strategies: vec![],
			close_long_strategies: vec![],
			close_short_strategies: vec![],
			command_subscribers: vec![],
			open_long_signals: kanal::bounded_async(100),
			open_short_signals: kanal::bounded_async(100),
			close_long_signals: kanal::bounded_async(100),
			close_short_signals: kanal::bounded_async(100)
		}
	}
	
	async fn recv_commands(r: &AsyncReceiver<StrategyEdge>, s: AsyncSender<ExecutionCommand>, command_type: ExecutionCommand) {
		while let Ok(_) = r.recv().await {
				s.send(command_type.clone()).await;
		}
	}
	
	pub fn with_long_entry_strategy<S: EventEmitter<StrategyEdge> + Clone + Send>(mut self, strategy: S) -> Self {
		self.open_long_strategies.push(Box::new(strategy));
		self
	}
	
	pub fn with_short_entry_strategy<S: EventEmitter<StrategyEdge> + Clone + Send> (mut self, strategy: S) -> Self {
		self.open_short_strategies.push(Box::new(strategy));
		self
	}
	
	pub fn with_long_exit_strategy<S: EventEmitter<StrategyEdge> + Clone + Send> (mut self, strategy: S) -> Self {
		self.close_long_strategies.push(Box::new(strategy));
		self
	}
	
	pub fn with_short_exit_strategy<S: EventEmitter<StrategyEdge> + Clone + Send> (mut self, strategy: S) -> Self {
		self.close_short_strategies.push(Box::new(strategy));
		self
	}
	
	pub fn with_entry_strategy<S: EventEmitter<StrategyEdge> + Clone + Send> (mut self, strategy: S) -> Self {
		self.open_long_strategies.push(Box::new(strategy.clone()));
		self.open_short_strategies.push(Box::new(strategy));
		self
	}
	
	pub fn with_exit_strategy<S: EventEmitter<StrategyEdge> + Clone > (mut self, strategy: S) -> Self {
		self.close_long_strategies.push(Box::new(strategy.clone()));
		self.close_short_strategies.push(Box::new(strategy));
		self
	}
	pub fn with_strategy<S: EventEmitter<StrategyEdge> + Clone + Send> (mut self, strategy: S) -> Self {
		self.open_long_strategies.push(Box::new(strategy.clone()));
		self.open_short_strategies.push(Box::new(strategy.clone()));
		self.close_long_strategies.push(Box::new(strategy.clone()));
		self.close_short_strategies.push(Box::new(strategy));
		self
	}
}