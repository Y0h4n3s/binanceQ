use kanal::{AsyncReceiver, AsyncSender};
use crate::events::{EventEmitter, EventSink};
use crate::types::TfTrades;
use async_trait::async_trait;
pub mod chop_directional;
pub mod random_strategy;
use dyn_clone::DynClone;
#[derive(Copy, Clone, PartialEq)]
pub enum StrategyEdge {
	Long,
	Short,
	CloseLong,
	CloseShort,
	Neutral,
}

#[async_trait]
pub trait SignalGenerator: DynClone + Send + Sync {
	async fn get_signal(&self) -> StrategyEdge;
}

