use async_std::sync::Arc;
use async_trait::async_trait;
use std::fmt::Debug;
use tokio::runtime::Runtime;
use async_broadcast::{Receiver, Sender};
use tokio::sync::{RwLock};
use tokio::task::JoinHandle;

pub trait EventSink<EventType: Clone + Debug + Send + Sync >: Send + Sync {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<EventType>>>;
    fn handle_event(
        &self,
        event_msg: EventType,
    ) -> anyhow::Result<JoinHandle<anyhow::Result<()>>>;
    fn working(&self) -> bool;
    fn set_working(&self, working: bool) -> anyhow::Result<()>;
    fn listen(&self) -> std::result::Result<(), anyhow::Error> {
        let runtime = Runtime::new()?;
        let task_set = tokio::task::LocalSet::new();
        let receiver = self.get_receiver();
        task_set.block_on(&runtime, async move {
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
                    Ok(event) => {
                        self.set_working(true)?;
                        self.handle_event(event)?.await??;
                        self.set_working(false)?;

                    }
                }
            }
    
            Ok::<(), anyhow::Error>(())
        })
    }
}

#[async_trait]
pub trait EventEmitter<EventType: Send + Sync + 'static> {
    fn get_subscribers(&self) -> Arc<RwLock<Sender<EventType>>>;
    async fn subscribe(&mut self, sender: Sender<EventType>) {
        let subs = self.get_subscribers();
        *subs.write().await = sender;
    }
    async fn shutdown(&mut self) {
        let subs = self.get_subscribers();
        std::mem::drop(subs.write().await);
    }
    async fn emit(&self) -> anyhow::Result<JoinHandle<()>>;
}
