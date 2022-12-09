use tokio::task::JoinHandle;
pub mod atr_study;
pub mod choppiness_study;
pub mod directional_index_study;






pub trait Study: EventSink<TfTrades> {
	const ID: StudyTypes;
	type Entry;
	fn log_history(&self) -> JoinHandle<()>;
	fn get_entry_for_tf(&self, tf: u64) -> Self::Entry;
	fn get_n_entries_for_tf(&self, n: u64, tf: u64) -> Vec<Self::Entry>;
	fn sentiment(&self) -> Sentiment;
	fn sentiment_with_one<T>(&self, other: T) -> Sentiment
		where T: Study;
	fn sentiment_with_two<T, U>(&self, other1: T, other2: U) -> Sentiment
		where T: Study, U: Study;
	
	
}

