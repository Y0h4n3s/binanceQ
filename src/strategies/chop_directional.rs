use kanal::AsyncSender;
use crate::EventEmitter;
use crate::strategies::StrategyEdge;

pub struct ChopDirectionalStrategy {
	subscribers: Vec<AsyncSender<StrategyEdge>>,
}

impl EventEmitter<StrategyEdge> for ChopDirectionalStrategy {
	fn subscribe(&mut self, sender: AsyncSender<StrategyEdge>) {
		self.subscribers.push(sender)
	}
	
	async fn emit(&self) {
	
	}
}