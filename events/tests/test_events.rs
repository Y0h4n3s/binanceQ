use std::time::Duration;
use async_std::sync::Arc;
use tokio::sync::broadcast::{Sender, Receiver};
use tokio::sync::{Notify, RwLock, SemaphorePermit};
use tokio::task::JoinHandle;
use binance_q_events::{EventSink, EventEmitter};
use async_trait::async_trait;

#[derive(Clone, Debug)]
struct DummyEvent {
	pub dummy_count: u8,
}

struct DummyEmitter {
	pub dummy_event_type: Arc<RwLock<DummyEvent>>,
	subscribers: Arc<RwLock<Sender<DummyEvent>>>,
}

#[derive(Clone)]
struct DummySink {
	final_event: Arc<RwLock<DummyEvent>>,
	receiver: Arc<RwLock<Receiver<DummyEvent>>>,
}
#[derive(Clone)]
struct DummySinkTwo {
	final_event: Arc<RwLock<DummyEvent>>,
	receiver: Arc<RwLock<Receiver<DummyEvent>>>,
}

impl DummyEmitter {
	pub fn new() -> Self {
		Self {
			dummy_event_type: Arc::new(RwLock::new(DummyEvent { dummy_count: 0 })),
			subscribers: Arc::new(RwLock::new(tokio::sync::broadcast::channel(1).0)),
		}
	}
}

impl DummySink {
	pub fn new(events: Receiver<DummyEvent>) -> Self {
		Self {
			final_event: Arc::new(RwLock::new(DummyEvent { dummy_count: 1 })),
			receiver: Arc::new(RwLock::new(events)),
		}
	}
}

impl DummySinkTwo {
	pub fn new(events: Receiver<DummyEvent>) -> Self {
		Self {
			final_event: Arc::new(RwLock::new(DummyEvent { dummy_count: 1 })),
			receiver: Arc::new(RwLock::new(events)),
		}
	}
}

#[async_trait]
impl EventEmitter<'_, DummyEvent> for DummyEmitter {
	fn get_subscribers(&self) -> Arc<RwLock<Sender<DummyEvent>>> {
		self.subscribers.clone()
	}
	
	async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
		let subs = self.get_subscribers();
		let event_type = self.dummy_event_type.clone();
		Ok(tokio::spawn(async move {
			loop {
				let mut event = event_type.write().await;
				if event.dummy_count > u8::MAX - 3 {
					break;
				}
				event.dummy_count += 1;
				let mut subs = subs.write().await;
				subs.send(event.clone()).unwrap();
			}
		}))
	}
}

impl EventSink<DummyEvent> for DummySink {
	fn get_receiver(&self) -> Arc<RwLock<Receiver<DummyEvent>>> {
		self.receiver.clone()
	}
	
	fn handle_event(&self, event_msg: DummyEvent) -> anyhow::Result<JoinHandle<()>> {
		let final_event = self.final_event.clone();
		
		Ok(tokio::spawn(async move {
			let mut f = final_event.write().await;
			
			f.dummy_count = event_msg.dummy_count;
			// println!("Fina?l event: {:?}", f.dummy_count);
		}))
	}
}

impl EventSink<DummyEvent> for DummySinkTwo {
	fn get_receiver(&self) -> Arc<RwLock<Receiver<DummyEvent>>> {
		self.receiver.clone()
	}
	
	fn handle_event(&self, event_msg: DummyEvent) -> anyhow::Result<JoinHandle<()>> {
		let final_event = self.final_event.clone();
		Ok(tokio::spawn(async move {
			let mut f = final_event.write().await;
			
			f.dummy_count = event_msg.dummy_count + 1;
			
			
		}))
	}
}


#[cfg(test)]
mod tests {
	use super::*;
	
	#[tokio::test]
	async fn test_events_throughput() -> anyhow::Result<()> {
		let (sender, receiver) = tokio::sync::broadcast::channel::<DummyEvent>(100000);
		
		let dummy_sink = DummySink::new(sender.subscribe());
		let dummy_sink_two = DummySinkTwo::new(receiver);
		let d_final = dummy_sink.final_event.clone();
		let d_final_two = dummy_sink_two.final_event.clone();
		let mut emitter = DummyEmitter::new();
		emitter.subscribe(sender.clone()).await;
		
		let mut threads = vec![std::thread::spawn(move || {
			let res = dummy_sink.listen();
		}), std::thread::spawn(move || {
			dummy_sink_two.listen();
		})];
		emitter.emit().await?.await;
		emitter.shutdown().await;
		std::mem::drop(sender);
		tokio::time::sleep(std::time::Duration::from_millis(10)).await;
		// tokio::time::sleep(std::time::Duration::from_secs(2)).await;
		for thread in threads {
			println!("Joining thread {}", thread.is_finished());
			// thread.join().unwrap();
		}
		
		assert!(emitter.dummy_event_type.read().await.dummy_count > u8::MAX - 20);
		assert!(d_final.read().await.dummy_count > u8::MAX - 20);
		assert!(d_final_two.read().await.dummy_count > u8::MAX - 10);
		Ok(())
	}
}












