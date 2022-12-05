use std::str::FromStr;
use crate::mongodb::models::TradeEntry;



pub fn to_precision(num: f64, precision: usize) -> f64 {
	f64::from_str(format!("{:.*}", precision, num).as_str()).unwrap()
}

pub fn to_tf_chunks(tf: u64, mut data: Vec<TradeEntry>) -> Vec<Vec<TradeEntry>> {
	if data.len() <= 0 {
		return vec![];
	}
	data.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
	
	let mut chunks = vec![];
	let mut timestamp = data[0].timestamp;
	let mut last_i = 0;
	for (i, trade) in data.clone().iter().enumerate() {
		if trade.timestamp - timestamp < tf * 1000 {
			continue
		}
		timestamp = trade.timestamp;
		chunks.push(data[last_i..i].to_vec());
		last_i = i;
	}
	if chunks.len() <= 0 {
		chunks.push(data);
	}
	chunks
	
}