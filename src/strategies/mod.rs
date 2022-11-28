pub mod chop_directional;

pub enum StrategyEdge {
	Long,
	Short,
	CloseLong,
	CloseShort,
	Neutral,
}

pub trait Strategy:  {
	fn decide(&self) -> StrategyEdge;
}