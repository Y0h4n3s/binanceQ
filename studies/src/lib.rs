use tokio::task::JoinHandle;
pub mod atr_study;
pub mod choppiness_study;
pub mod directional_index_study;
use binance_q_types::{StudyTypes, Sentiment, TfTrades};
use binance_q_events::EventSink;
use async_trait::async_trait;


#[async_trait]
pub trait Study: EventSink<TfTrades> {
	const ID: StudyTypes;
	type Entry;
	fn log_history(&self) -> JoinHandle<()>;
	async fn get_entry_for_tf(&self, tf: u64) -> Option<Self::Entry>;
	async fn get_n_entries_for_tf(&self, n: u64, tf: u64) -> Option<Vec<Self::Entry>>;
	fn sentiment(&self) -> Sentiment;
	fn sentiment_with_one<T>(&self, other: T) -> Sentiment
		where T: Study;
	fn sentiment_with_two<T, U>(&self, other1: T, other2: U) -> Sentiment
		where T: Study, U: Study;
	
	
}

