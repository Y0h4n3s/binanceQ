use std::str::FromStr;
use std::time::Duration;
use crate::errors::{Error, ErrorKind, Result};
use binance::errors::Result as BinanceResult;
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