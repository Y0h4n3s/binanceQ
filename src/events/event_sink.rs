use anyhow::Error;
/// This module defines the `EventSink` trait, which is used to handle events
/// asynchronously. It provides a mechanism to listen for events and process
/// them using the `handle_event` method.
use async_broadcast::Receiver;
use async_std::sync::Arc;
use async_trait::async_trait;
use log::info;
use std::fmt::Debug;
use tokio::sync::Notify;
use tracing::{debug, error};

#[async_trait]
/// The `EventSink` trait defines the interface for handling events of type `EventType`.
/// It requires implementing the `get_receiver` and `handle_event` methods, and provides
/// a default implementation for the `listen` method to continuously process incoming events.
pub trait EventSink<EventType: Clone + Debug + Send + Sync + 'static>:
    Send + Sync + 'static
{
    /// Returns a receiver for the event channel, allowing the sink to receive events.
    fn get_receiver(&self) -> Receiver<(EventType, Option<Arc<Notify>>)>;

    async fn name(&self) -> String;
    /// Handles an incoming event of type `EventType`.
    /// Implementations should define how to process the event.
    async fn handle_event(&self, event_msg: EventType) -> anyhow::Result<()>;

    /// Listens for incoming events and processes them using the `handle_event` method.
    /// This method runs in an asynchronous loop and handles errors that occur during event processing.
    fn listen(self: Arc<Self>) -> Result<(), Error> {
        let runtime = tokio::runtime::Handle::current();
        let mut receiver = self.get_receiver();
        runtime.spawn(async move {
            let name = self.name().await;

            loop {
                match receiver.recv().await {
                    Err(async_broadcast::RecvError::Closed) => {
                        debug!("[-] Sender closed on {}. Shutting down", &name);
                        receiver.close();
                        break;
                    }
                    Err(async_broadcast::RecvError::Overflowed(e)) => {
                        error!("[-] Broadcast error: Overflowed {} on {}", e, &name);
                        continue;
                    }
                    Ok((event, notify)) => {
                        if let Err(e) = self.handle_event(event).await {
                            error!("[-] Error handling event: {} on {}", e, &name);
                        }
                        if let Some(n) = notify {
                            n.notify_one()
                        }
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{anyhow, Error};
    use async_broadcast::{broadcast, Receiver};
    use async_std::sync::Arc;
    use async_trait::async_trait;
    use std::fmt::Debug;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Notify;

    #[derive(Clone, Debug, PartialEq)]
    struct MockEvent {
        pub message: String,
    }

    struct MockEventSink {
        receiver: Receiver<(MockEvent, Option<Arc<Notify>>)>,
        handled_events: Arc<AtomicUsize>,
    }

    impl MockEventSink {
        pub fn new(receiver: Receiver<(MockEvent, Option<Arc<Notify>>)>) -> Self {
            Self {
                receiver,
                handled_events: Arc::new(AtomicUsize::new(0)),
            }
        }

        pub fn get_handled_events_count(&self) -> usize {
            self.handled_events.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl EventSink<MockEvent> for MockEventSink {
        fn get_receiver(&self) -> Receiver<(MockEvent, Option<Arc<Notify>>)> {
            self.receiver.clone()
        }
        async fn name(&self) -> String {
            "MockEventSink".to_string()
        }

        async fn handle_event(&self, event_msg: MockEvent) -> anyhow::Result<()> {
            if event_msg.message.is_empty() {
                return Err(anyhow!("MockEventSink got empty message"));
            };
            self.handled_events.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_event_sink_event_err() -> Result<(), Error> {
        let (sender, receiver) = broadcast::<(MockEvent, Option<Arc<Notify>>)>(10);
        let event_sink = Arc::new(MockEventSink::new(receiver));

        event_sink.clone().listen()?;

        let notifier = Arc::new(Notify::new());
        let n1 = notifier.notified();
        let event = MockEvent {
            message: "".to_string(),
        };
        sender
            .broadcast((event.clone(), Some(notifier.clone())))
            .await?;

        n1.await;

        assert_eq!(event_sink.get_handled_events_count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_event_sink_success() -> Result<(), Error> {
        let (sender, receiver) = broadcast::<(MockEvent, Option<Arc<Notify>>)>(10);
        let event_sink = Arc::new(MockEventSink::new(receiver));

        event_sink.clone().listen()?;

        let notifier = Arc::new(Notify::new());
        let n1 = notifier.notified();
        let n2 = notifier.notified();
        let event = MockEvent {
            message: "Event 1".to_string(),
        };
        sender
            .broadcast((event.clone(), Some(notifier.clone())))
            .await?;

        let event = MockEvent {
            message: "Event 2".to_string(),
        };
        sender
            .broadcast((event.clone(), Some(notifier.clone())))
            .await?;
        n1.await;
        n2.await;

        assert_eq!(event_sink.get_handled_events_count(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_event_sink_overflow() -> Result<(), Error> {
        let (mut sender, receiver) = broadcast::<(MockEvent, Option<Arc<Notify>>)>(1);
        let event_sink = Arc::new(MockEventSink::new(receiver));
        sender.set_overflow(true);

        event_sink.clone().listen()?;

        let event = MockEvent {
            message: "Event 1".to_string(),
        };
        sender.broadcast((event.clone(), None)).await?;

        let event2 = MockEvent {
            message: "Event 2".to_string(),
        };
        sender.broadcast((event2.clone(), None)).await?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(event_sink.get_handled_events_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_event_sink_closed() -> Result<(), Error> {
        let (sender, receiver) = broadcast::<(MockEvent, Option<Arc<Notify>>)>(10);

        let event_sink = Arc::new(MockEventSink::new(receiver));
        event_sink.clone().listen()?;

        drop(sender);

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(event_sink.get_handled_events_count(), 0);

        Ok(())
    }
}
