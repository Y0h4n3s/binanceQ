// use std::time::{SystemTime, UNIX_EPOCH};
//
// use binance::account::Account;
// use binance::api::Binance;
// use binance::futures::account::FuturesAccount;
// use binance::futures::general::FuturesGeneral;
// use binance::futures::market::FuturesMarket;
// use binance::futures::model::{AggTrade, ExchangeInformation, Symbol};
// use binance::futures::model::AggTrades::AllAggTrades;
// use binance::futures::userstream::FuturesUserStream;
// use binance::savings::Savings;
// use binance::userstream::UserStream;
// use image::RgbImage;
// use ndarray::{ Array2, Array3};
//
// use crate::AccessKey;
// use crate::helpers::{request_with_retries, to_precision};
//
// pub struct MarketClassiferConfig {
// }
// pub struct MarketClassifer {
// 	pub config: MarketClassiferConfig,
// 	pub futures_account: FuturesAccount,
// 	pub account: Account,
// 	pub futures_market: FuturesMarket,
// 	pub binance: FuturesGeneral,
// 	pub user_stream: UserStream,
// 	pub futures_user_stream: FuturesUserStream,
// 	pub symbols: Vec<Symbol>,
// 	pub savings: Savings
// }
//
// impl MarketClassifer {
// 	pub fn new(key: AccessKey, config: MarketClassiferConfig) -> Self {
// 		let futures_account =
// 			  FuturesAccount::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
// 		let binance = FuturesGeneral::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
// 		let account = Account::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
// 		let user_stream = UserStream::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
// 		let futures_user_stream =
// 			  FuturesUserStream::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
// 		let futures_market =
// 			  FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
// 		let savings = Savings::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
// 		let symbols =
// 			  match request_with_retries::<ExchangeInformation>(5, || binance.exchange_info()) {
// 				  Ok(e) => e.symbols,
// 				  Err(e) => panic!("Error getting symbols: {}", e),
// 			  };
//
// 		MarketClassifer {
// 			config,
// 			futures_account,
// 			binance,
// 			account,
// 			futures_market,
// 			user_stream,
// 			futures_user_stream,
// 			symbols,
// 			savings
// 		}
// 	}
//
// 	pub async fn get_trades_until(&self, until: u128, symbol: &str) -> Vec<AggTrade> {
// 		let mut trades = vec![];
// 		let mut last_id = None;
// 		loop {
// 			if let Ok(AllAggTrades(mut t)) = self.futures_market.get_agg_trades(symbol.clone(), last_id, None, None,Some(1000)) {
// 				trades.append(&mut t);
// 			}
// 			trades.sort_by(|a, b| a.time.cmp(&b.time));
// 			last_id = Some(trades.first().unwrap().agg_id - 1000);
//
// 			println!("{} {}", until, trades.first().unwrap().time);
// 			if until > trades.first().unwrap().time as u128 {
// 				break;
// 			}
// 		}
// 		trades
// 	}
//
// 	pub fn recent_high(&self, prices: &Vec<f64>) -> f64 {
// 		let mut high = *prices.last().unwrap();
// 		for i in prices.len()..0 {
// 			if i > prices.len() - 5 {
// 				continue;
// 			}
// 			if i < 4 && i > prices.len() - 5 {
// 				high = prices.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap().clone();
// 				break;
// 			}
// 			if i < 4 {
// 				break;
// 			}
// 			if prices[i] > prices[i - 1] && prices[i] > prices[i - 2]  && prices[i] > prices[i - 3] && prices[i] > prices[i - 4] &&prices[i] > prices[i+1] && prices[i] > prices[i+2] && prices[i] > prices[i + 3] && prices[i] > prices[i + 3]  {
// 				high = prices[i];
// 				break;
// 			}
// 		}
// 		high
// 	}
//
// 	pub fn recent_low(&self, prices: &Vec<f64>) -> f64 {
// 		let mut high = *prices.last().unwrap();
// 		for i in prices.len()..0 {
// 			if i > prices.len() - 5 {
// 				continue;
// 			}
// 			if i < 4 && i > prices.len() - 5 {
// 				high = prices.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap().clone();
// 				break;
// 			}
// 			if i < 4 {
// 				break;
// 			}
// 			if prices[i] < prices[i - 1] && prices[i] < prices[i - 2]  && prices[i] < prices[i-3] && prices[i] < prices[i-4] && prices[i] < prices[i+1] && prices[i] < prices[i+2]  && prices[i] < prices[i+3] && prices[i] < prices[i+4]  {
// 				high = prices[i];
// 				break;
// 			}
// 		}
// 		high
// 	}
// 	pub fn hh_ll_index(&self, prices: &Array2<f64>) -> (f64, f64){
// 		let mut recent_high = 0.0;
// 		let mut recent_low = 0.0;
// 		let mut last_price = 0.0;
// 		let mut high_breaks = 0.0;
// 		let mut low_breaks = 0.0;
// 		let mut prices_so_far = vec![];
// 		for i in 0..prices.shape()[0] {
// 			let row = prices.row(i);
// 			let price = row.iter().find(|p| **p != 0.0).unwrap();
// 			prices_so_far.push(*price);
// 			if recent_high == 0.0 && recent_low == 0.0 && last_price == 0.0 {
// 				recent_high = *price;
// 				recent_low = *price;
// 				last_price = *price;
// 				continue
// 			}
//
//
// 			if *price > recent_high  {
// 				high_breaks += 1.0;
// 			}
// 			if *price < recent_low  {
// 				low_breaks += 1.0;
// 			}
// 			recent_high = self.recent_high(&prices_so_far);
// 			recent_low = self.recent_low(&prices_so_far);
// 			last_price = *price;
// 			//println!("{} {} {} {} {}", price, recent_high, recent_low, high_breaks, low_breaks);
// 		}
//
// 		println!("High breaks: {}", high_breaks);
// 		println!("Low breaks: {}", low_breaks);
// 		return (high_breaks,  low_breaks);
// 	}
// 	pub fn remove_consecutive_duplicated<T>(&self, array: &mut Vec<T>) -> Vec<T>
// 		where T: PartialEq + Clone {
// 		let mut i = 0;
// 		while i < array.len() - 1 {
// 			if array[i] == array[i + 1] {
// 				array.remove(i);
// 			} else {
// 				i += 1;
// 			}
// 		}
// 		array.to_vec()
// 	}
//
// 	fn array_to_image(&self, arr: Array3<u8>) -> RgbImage {
// 		assert!(arr.is_standard_layout());
//
// 		let (height, width, _) = arr.dim();
// 		let raw = arr.into_raw_vec();
//
// 		RgbImage::from_raw(width as u32, height as u32, raw)
// 			  .expect("container should have the right size for the image dimensions")
// 	}
// 	pub async fn classify(&self) {
// 		for symbol in &self.symbols {
// 			if symbol.symbol != "DOGEUSDT" {
// 				continue;
// 			}
// 			let mut trades = self.get_trades_until(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() - 1000 * 60 * 60 * 2, symbol.symbol.as_str()).await.iter().map(|t| to_precision(t.price, symbol.price_precision as usize  - 1)).collect::<Vec<f64>>();
// 			println!("Trades: {:?}", symbol.price_precision);
// 			trades = self.remove_consecutive_duplicated(&mut trades);
// 			println!("{} {}", symbol.symbol, trades.len());
// 			let highest_price = trades.iter().max_by(|a, b| a.partial_cmp(&b).unwrap()).unwrap();
// 			let lowest_price = trades.iter().max_by(|a, b| b.partial_cmp(&a).unwrap()).unwrap();
// 			println!("{} {}", highest_price, lowest_price);
// 			let j_len = ((highest_price - lowest_price) / (trades.get(0).unwrap() - trades.get(1).unwrap()).abs()) as usize;
// 			println!("i length: {} j Length: {}", trades.len(),  j_len);
// 			let mut price_array = Array3::<u8>::zeros((trades.len(), j_len + 1, 3));
//
// 			for (i,trade) in trades.iter().enumerate() {
// 				price_array[[i, ((trade - lowest_price) / (trades.get(0).unwrap() - trades.get(1).unwrap()).abs()) as usize, 0 ]] = 255_u8;
// 				price_array[[i, ((trade - lowest_price) / (trades.get(0).unwrap() - trades.get(1).unwrap()).abs()) as usize, 1 ]] = 255_u8;
// 				price_array[[i, ((trade - lowest_price) / (trades.get(0).unwrap() - trades.get(1).unwrap()).abs()) as usize, 2 ]] = 255_u8;
// 			}
// 			let image = self.array_to_image(price_array);
// 			image.save("out.png").unwrap();
// 			// let (hh, ll) = self.hh_ll_index(&price_array);
// 			// println!("{:?}", (hh * 100.0)/(hh + ll));
// 			break;
// 		}
// 	}
// }