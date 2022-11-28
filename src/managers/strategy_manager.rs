use futures::stream::FuturesUnordered;
use kanal::{AsyncReceiver, AsyncSender};
use crate::events::EventEmitter;
use crate::GlobalConfig;
use crate::managers::risk_manager::ExecutionCommand;
use crate::strategies::{SignalGenerator, Strategy, StrategyEdge};



pub struct StrategyManager<S: SignalGenerator + EventEmitter<StrategyEdge>> {
	global_config: GlobalConfig,
	pub open_long_strategies: Vec<S>,
	pub open_short_strategies: Vec<S>,
	pub close_long_strategies: Vec<S>,
	pub close_short_strategies: Vec<S>,
	pub command_subscribers: Vec<AsyncSender<ExecutionCommand>>,
	open_long_signals: (AsyncSender<StrategyEdge>, AsyncReceiver<StrategyEdge>),
	open_short_signals: (AsyncSender<StrategyEdge>, AsyncReceiver<StrategyEdge>),
	close_long_signals: (AsyncSender<StrategyEdge>, AsyncReceiver<StrategyEdge>),
	close_short_signals: (AsyncSender<StrategyEdge>, AsyncReceiver<StrategyEdge>)
}

impl EventEmitter<ExecutionCommand> for StrategyManager<T> {
	fn subscribe(&mut self, sender: AsyncSender<ExecutionCommand>) {
		self.command_subscribers.push(sender);
	}
	
	async fn emit(&self) {
		let command_subscribers = self.command_subscribers.clone();
		let mut signal_workers = vec![];
		for mut s in self.open_long_strategies {
			s.subscribe(self.open_long_signals.0.clone());
			signal_workers.push(s.emit());
			signal_workers.push(async move {
				while let Some(_) = self.open_long_signals.1.recv().await {
					for sender in command_subscribers {
						sender.send(ExecutionCommand::OpenLongPosition).await;
					}
				}
			});
			
		}
		for mut s in self.open_short_strategies {
			s.subscribe(self.open_short_signals.0.clone());
			signal_workers.push(s.emit());
			signal_workers.push(async move {
				while let Some(_) = self.open_short_signals.1.recv().await {
					for sender in command_subscribers {
						sender.send(ExecutionCommand::OpenShortPosition).await;
					}
				}
			});
		}
		for mut s in self.close_long_strategies {
			s.subscribe(self.close_long_signals.0.clone());
			signal_workers.push(s.emit());
			signal_workers.push(async move {
				while let Some(_) = self.close_long_signals.1.recv().await {
					for sender in command_subscribers {
						sender.send(ExecutionCommand::CloseLongPosition).await;
					}
				}
			});
		}
		for mut s in self.close_short_strategies {
			s.subscribe(self.close_short-signals.0.clone());
			signal_workers.push(s.emit());
			signal_workers.push(async move {
				while let Some(_) = self.close_short_signals.1.recv().await {
					for sender in command_subscribers {
						sender.send(ExecutionCommand::CloseShortPosition).await;
					}
				}
			});
		}
		futures::future::join_all(signal_workers).await;
	}
}

impl <S: Strategy> StrategyManager<S> {
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
	
	pub fn with_long_entry_strategy<S: Strategy>(mut self, strategy: S) -> Self {
		self.open_long_strategies.push(strategy);
		self
	}
	
	pub fn with_short_entry_strategy<S: Strategy>(mut self, strategy: S) -> Self {
		self.open_short_strategies.push(strategy);
		self
	}
	
	pub fn with_long_exit_strategy<S: Strategy>(mut self, strategy: S) -> Self {
		self.close_long_strategies.push(strategy);
		self
	}
	
	pub fn with_short_exit_strategy<S: Strategy>(mut self, strategy: S) -> Self {
		self.close_short_strategies.push(strategy);
		self
	}
	
	pub fn with_entry_strategy<S: Strategy>(mut self, strategy: S) -> Self {
		self.open_long_strategies.push(strategy.clone());
		self.open_short_strategies.push(strategy);
		self
	}
	
	pub fn with_exit_strategy<S: Strategy>(mut self, strategy: S) -> Self {
		self.close_long_strategies.push(strategy.clone());
		self.close_short_strategies.push(strategy);
		self
	}
	pub fn with_strategy<S: Strategy>(mut self, strategy: S) -> Self {
		self.open_long_strategies.push(strategy.clone());
		self.open_short_strategies.push(strategy.clone());
		self.close_long_strategies.push(strategy.clone());
		self.close_short_strategies.push(strategy);
		self
	}
}