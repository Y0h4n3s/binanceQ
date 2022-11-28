use kanal::{AsyncReceiver, AsyncSender};
use crate::events::{EventEmitter, EventSink};
use crate::types::TfTrades;
use async_trait::async_trait;
pub mod chop_directional;

#[derive(Copy, Clone)]
pub enum StrategyEdge {
	Long,
	Short,
	CloseLong,
	CloseShort,
	Neutral,
}
#[async_trait]
pub trait SignalGenerator {
	fn subscribers(&self) -> Vec<AsyncSender<StrategyEdge>>;
	async fn get_signal(&self) -> StrategyEdge;
}

