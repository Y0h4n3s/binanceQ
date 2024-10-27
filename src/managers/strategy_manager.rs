use std::collections::VecDeque;
use std::sync::Arc;
use async_broadcast::{InactiveReceiver, Sender};
use async_trait::async_trait;
use tokio::sync::{Notify, RwLock};
use crate::types::{ExecutionCommand, GlobalConfig, Kline};


#[async_trait]
pub trait SignalGenerator {

}
#[derive(Clone)]
pub struct StrategyManager<RiskManager> {
    global_config: GlobalConfig,
    pub command_subscribers: Arc<RwLock<Sender<(ExecutionCommand, Option<Arc<Notify>>)>>>,
    signal_generators: Arc<RwLock<Vec<Box<dyn SignalGenerator>>>>,
    klines: InactiveReceiver<(Kline, Option<Arc<Notify>>)>,
    risk_manager: RiskManager,
}
