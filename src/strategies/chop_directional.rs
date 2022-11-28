use kanal::AsyncSender;
use crate::EventEmitter;
use crate::strategies::{SignalGenerator, StrategyEdge};

pub struct ChopDirectionalStrategy {
	subscribers: Vec<AsyncSender<StrategyEdge>>,
}

impl EventEmitter<StrategyEdge> for ChopDirectionalStrategy {
	fn subscribe(&mut self, sender: AsyncSender<StrategyEdge>) {
		self.subscribers.push(sender)
	}
	
	async fn emit(&self) {
		let signal = self.get_signal().await;
		for subscriber in self.subscribers {
			subscriber.send(signal).await
		}
	}
}

impl SignalGenerator for ChopDirectionalStrategy {
	fn subscribers(&self) -> Vec<AsyncSender<StrategyEdge>> {
		todo!()
	}
	
	async fn get_signal(&self) -> StrategyEdge {
		// calculate here
		todo!()
	}
}