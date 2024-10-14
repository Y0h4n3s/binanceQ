use async_broadcast::Sender;
use async_std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;

#[async_trait]
pub trait EventEmitter<EventType: Send + Sync + 'static> {
    fn get_subscribers(&self) -> Arc<RwLock<Sender<(EventType, Option<Arc<Notify>>)>>>;
    async fn subscribe(&mut self, sender: Sender<(EventType, Option<Arc<Notify>>)>) {
        let subs = self.get_subscribers();
        *subs.write().await = sender;
    }
    #[allow(dead_code)]
    async fn shutdown(&mut self) {
        let subs = self.get_subscribers();
        drop(subs.write().await);
    }
    async fn emit(&self) -> anyhow::Result<JoinHandle<()>>;
}

#[cfg(test)]
mod tests {
    use crate::events::EventSink;
    use async_broadcast::{Receiver, Sender};
    use async_std::sync::Arc;
    use async_trait::async_trait;
    use tokio::sync::RwLock;
    use tokio::task::JoinHandle;

    #[derive(Clone, Debug)]
    struct DummyEvent {
        pub dummy_count: u16,
    }

    type ArcLock<T> = Arc<RwLock<T>>;
    struct DummyEmitter {
        pub dummy_event_type: ArcLock<DummyEvent>,
        subscribers: ArcLock<Sender<DummyEvent>>,
    }

    #[derive(Clone)]
    struct DummySink {
        final_event: ArcLock<DummyEvent>,
        receiver: ArcLock<Receiver<DummyEvent>>,
        working: Arc<std::sync::RwLock<bool>>,
    }
    #[derive(Clone)]
    struct DummySinkTwo {
        final_event: ArcLock<DummyEvent>,
        receiver: ArcLock<Receiver<DummyEvent>>,
        working: Arc<std::sync::RwLock<bool>>,
    }

    impl DummyEmitter {
        pub fn new() -> Self {
            Self {
                dummy_event_type: Arc::new(RwLock::new(DummyEvent { dummy_count: 0 })),
                subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
            }
        }
    }

    impl DummySink {
        pub fn new(events: Receiver<DummyEvent>) -> Self {
            Self {
                final_event: Arc::new(RwLock::new(DummyEvent { dummy_count: 1 })),
                receiver: Arc::new(RwLock::new(events)),
                working: Arc::new(std::sync::RwLock::new(false)),
            }
        }
    }

    impl DummySinkTwo {
        pub fn new(events: Receiver<DummyEvent>) -> Self {
            Self {
                final_event: Arc::new(RwLock::new(DummyEvent { dummy_count: 1 })),
                receiver: Arc::new(RwLock::new(events)),
                working: Arc::new(std::sync::RwLock::new(false)),
            }
        }
    }

    #[async_trait]
    impl EventEmitter<DummyEvent> for DummyEmitter {
        fn get_subscribers(&self) -> Arc<RwLock<Sender<DummyEvent>>> {
            self.subscribers.clone()
        }

        async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
            let subs = self.get_subscribers();
            let event_type = self.dummy_event_type.clone();
            Ok(tokio::spawn(async move {
                loop {
                    let mut event = event_type.write().await;
                    if event.dummy_count > u16::MAX - 3 {
                        break;
                    }
                    event.dummy_count += 1;
                    let subs = subs.write().await;
                    subs.broadcast(event.clone()).await.unwrap();
                }
            }))
        }
    }

    impl EventSink<DummyEvent> for DummySink {
        fn get_receiver(&self) -> Arc<RwLock<Receiver<DummyEvent>>> {
            self.receiver.clone()
        }
        fn working(&self) -> bool {
            *self.working.read().unwrap()
        }
        fn set_working(&self, working: bool) -> anyhow::Result<()> {
            *self.working.write().unwrap() = working;
            Ok(())
        }
        fn handle_event(
            &self,
            event_msg: DummyEvent,
        ) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
            let final_event = self.final_event.clone();

            Ok(tokio::spawn(async move {
                let mut f = final_event.write().await;

                f.dummy_count = event_msg.dummy_count;
                Ok(())
                // println!("Fina?l event: {:?}", f.dummy_count);
            }))
        }
    }

    impl EventSink<DummyEvent> for DummySinkTwo {
        fn get_receiver(&self) -> Arc<RwLock<Receiver<DummyEvent>>> {
            self.receiver.clone()
        }
        fn working(&self) -> bool {
            *self.working.read().unwrap()
        }
        fn set_working(&self, working: bool) -> anyhow::Result<()> {
            *self.working.write().unwrap() = working;
            Ok(())
        }
        fn handle_event(
            &self,
            event_msg: DummyEvent,
        ) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
            let final_event = self.final_event.clone();
            Ok(tokio::spawn(async move {
                let mut f = final_event.write().await;

                f.dummy_count = event_msg.dummy_count + 1;
                Ok(())
            }))
        }
    }

    use super::*;

    #[tokio::test]
    async fn test_events_throughput() -> anyhow::Result<()> {
        let (sender, receiver) = async_broadcast::broadcast::<DummyEvent>(100);

        let dummy_sink = DummySink::new(sender.new_receiver());
        let dummy_sink_two = DummySinkTwo::new(receiver);
        let d_final = dummy_sink.final_event.clone();
        let d_final_two = dummy_sink_two.final_event.clone();
        let mut emitter = DummyEmitter::new();
        emitter.subscribe(sender.clone()).await;

        std::thread::spawn(move || {
            dummy_sink.listen().unwrap();
            println!("Sink 1 done");
        });
        std::thread::spawn(move || {
            dummy_sink_two.listen().unwrap();
        });
        emitter.emit().await?.await.unwrap();
        emitter.shutdown().await;
        while !sender.is_empty() {
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }
        std::mem::drop(sender);

        assert_eq!(
            emitter.dummy_event_type.read().await.dummy_count,
            u16::MAX - 2
        );
        assert_eq!(d_final.read().await.dummy_count, u16::MAX - 2);
        assert_eq!(d_final_two.read().await.dummy_count, u16::MAX - 1);
        Ok(())
    }
}
