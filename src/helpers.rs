use std::str::FromStr;
use std::time::Duration;
use crate::errors::{Error, ErrorKind, Result};
use binance::errors::Result as BinanceResult;
use crate::mongodb::models::TradeEntry;

pub fn request_with_retries<R>(retries: usize, request: impl Fn() -> BinanceResult<R>) -> Result<R>
{
	let mut tries = 0;
	loop {
		if tries > retries {
			return Err("Too many retries".into());
		}
		match request() {
			Ok(r) => return Ok(r),
			Err(e) => {
				tries += 1;
				if tries > retries {
					return Err(ErrorKind::BinanceError(e).into());
				}
				std::thread::sleep(Duration::from_secs(1));
			}
		}
	}
	
}

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
	chunks
	
}