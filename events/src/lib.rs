use async_std::io::WriteExt;
use async_std::sync::Arc;
use async_trait::async_trait;
use std::fmt::Debug;
use std::future::Future;
use std::rc::Rc;
use tokio::runtime::Runtime;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{Notify, RwLock, SemaphorePermit};
use tokio::task::JoinHandle;
use tokio::time::Duration;

pub trait EventSink<EventType: Clone + Debug + Send + Sync + 'static>: Send + Sync {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<EventType>>>;
    // make sure to drop the permit when done
    fn handle_event(
        &self,
        event_msg: EventType,
    ) -> anyhow::Result<JoinHandle<()>>;

    fn listen(&self) -> std::result::Result<(), anyhow::Error> {
        let runtime = Runtime::new()?;
        let task_set = tokio::task::LocalSet::new();
        let mut receiver = self.get_receiver();
        task_set.block_on(&runtime, async move {
            // handle 100 events at a time
            let sender_closed = Arc::new(RwLock::new(false));
            let mut pool: Vec<JoinHandle<()>> = Vec::new();
            
            loop {
                let mut w = receiver.write().await;
    
                if *sender_closed.read().await && w.is_empty() {
                    for fut in pool {
                        fut.await;
                    }
                    break;
                }
                if pool.len() > 10 {
                    let mut new_pool = Vec::new();
                    
                    for  fut in pool {
                        if fut.is_finished() {
                            std::mem::drop(fut);
                            continue;
                        }
                        new_pool.push(fut);
                    }
                    pool = new_pool;
                    if pool.len() > 10 {
                        continue;
                    }
                }
                match w.recv().await {
                    
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        let mut s = sender_closed.write().await;
                        *s = true;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Ok(event) => {
                        pool.push(self.handle_event(event)?);
                    }
                }
            }
    
            Ok::<(), anyhow::Error>(())
        });
        Ok(())
    }
}

#[async_trait]
pub trait EventEmitter<'a, EventType: Send + Sync + 'static> {
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
