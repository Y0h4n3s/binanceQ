use async_broadcast::Receiver;
use async_std::sync::Arc;
use std::fmt::Debug;
use anyhow::Error;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;

pub trait EventSink<EventType: Clone + Debug + Send + Sync + 'static>: Send + Sync + 'static {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<(EventType, Option<Arc<Notify>>)>>>;
    fn handle_event(&self, event_msg: EventType) -> anyhow::Result<JoinHandle<anyhow::Result<()>>>;
    fn working(&self) -> bool;
    fn set_working(&self, working: bool) -> anyhow::Result<()>;
    fn listen(self: Arc<Self>) -> Result<(), Error> {
        let runtime = tokio::runtime::Handle::current();
        let receiver = self.get_receiver();
        runtime.spawn(async move {
            let sender_closed = Arc::new(RwLock::new(false));
            loop {
                let mut w = receiver.write().await;

                if *sender_closed.read().await && w.is_empty() {
                    break;
                }
                match w.recv().await {
                    Err(async_broadcast::RecvError::Closed) => {
                        let mut s = sender_closed.write().await;
                        *s = true;
                    }
                    Err(async_broadcast::RecvError::Overflowed(_)) => {
                        continue;
                    }
                    Ok((event, notify)) => {
                        self.set_working(true)?;
                        if let Ok(f) = self.handle_event(event) {
                            if let Ok(_) = f.await {
                                if let Some(n) = notify {
                                    n.notify_one()
                                }
                            }
                        }

                        self.set_working(false)?;
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        });
        Ok::<(), anyhow::Error>(())

    }
}
