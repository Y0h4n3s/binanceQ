// // let account: Account = Binance::new(api_key.clone(), secret_key.clone());
// // let market: Market = Binance::new(api_key.clone(), secret_key.clone());
// // let market: FuturesMarket = Binance::new(api_key.clone(), secret_key.clone());
// // let account: FuturesAccount = Binance::new(api_key.clone(), secret_key.clone());
// // // println!("{:?} {:?}", market.get_price(MARKET).unwrap(), market.get_24h_price_stats(MARKET).unwrap());
// // let market_index = std::env::args().nth(1).unwrap_or("0".to_string()).parse::<usize>().unwrap();
// //
// // let mut config = Config {
// //     stop_loss_percent: 0.3,
// //     target_percent: 0.6,
// //     stop_loss: 0.0,
// //     target: 0.0,
// //     price_precision: MARKET_PRICE_PRECISION[market_index] as usize,
// //     qty_precision: MARKET_QTY_PRECISION[market_index] as usize,
// //     traded_market: MARKET[market_index].to_string(),
// //     margin: 40.0,
// // };
// //
// // 'main_loop: loop {
// //     match listen_for_key() {
// //         'a' => {
// //             let market_price = market.get_price(&config.traded_market).unwrap();
// //
// //             let mut stop_diff = get_stop_range(&config, &market_price);
// //             let mut target_diff = get_target_range(&config, &market_price);
// //             config.stop_loss = to_precision(market_price.price - stop_diff, config.price_precision);
// //             config.target = to_precision(market_price.price + target_diff, config.price_precision);
// //             println!("Going long");
// //             long(&account, &market, &config, &market_price);
// //         }
// //         'd' => {
// //             let market_price = market.get_price(&config.traded_market).unwrap();
// //
// //             let mut stop_diff = get_stop_range(&config, &market_price);
// //             let mut target_diff = get_target_range(&config, &market_price);
// //
// //             config.stop_loss = to_precision(market_price.price + stop_diff, config.price_precision);
// //             config.target = to_precision(market_price.price - target_diff, config.price_precision);
// //             println!("Going short");
// //             short(&account, &market, &config, &market_price);
// //         }
// //         's' => {
// //             let market_price = market.get_price(&config.traded_market).unwrap();
// //             let mut stop_diff = get_stop_range(&config, &market_price);
// //             let mut target_diff = get_target_range(&config, &market_price);
// //             println!("{} {}", stop_diff, target_diff);
// //             println!("LONG");
// //             println!("-------------  {}", to_precision(market_price.price + target_diff, config.price_precision));
// //             println!("|\n|\n|");
// //             println!("-------------  {}", market_price.price);
// //             println!("|");
// //             println!("-------------  {}", to_precision(market_price.price - stop_diff, config.price_precision));
// //             println!("\n\n");
// //             println!("SHORT");
// //             println!("-------------  {}", to_precision(market_price.price + stop_diff, config.price_precision));
// //             println!("|");
// //             println!("-------------  {}", market_price.price);
// //             println!("|\n|\n|");
// //             println!("-------------  {}",to_precision(market_price.price - target_diff, config.price_precision));
// //         }
// //         'p' => {
// //             struct PricePoint {
// //                 pub index: i64,
// //                 pub percent_change: Decimal,
// //                 pub symbol: String,
// //                 pub volume: Decimal,
// //                 pub price: Decimal,
// //             }
// //             let mut history: Vec<Vec<PricePoint>> = vec![];
// //             let mut index: i64 = 1;
// //             let calc_durations = vec![Duration::from_secs(300), Duration::from_secs(3600), Duration::from_secs(3600 * 4), Duration::from_secs(3600 * 24)];
// //             let check_speed = Duration::from_millis(100);
// //             loop {
// //                 // let mut price_stats = market.get_all_24h_price_stats().unwrap();
// //                 // price_stats.sort_by(|a, b| if Decimal::from_str(&a.price_change_percent).unwrap().ge(&Decimal::from_str(&a.price_change_percent).unwrap()) {Ordering::Greater} else { Ordering::Less });
// //                 // let mut price_points = vec![];
// //                 // for price_stat in price_stats {
// //                 //     price_points.push(PricePoint {
// //                 //         index,
// //                 //         percent_change:Decimal::from_str(&price_stat.price_change_percent).unwrap(),
// //                 //         symbol: price_stat.symbol,
// //                 //         volume: Decimal::from_str(&price_stat.price_change_percent).unwrap(),
// //                 //         price: Decimal::from_str(&price_stat.last_price.to_string()).unwrap()
// //                 //     });
// //                 //     println!("{}  {}  {} https://www.binance.com/en/futures/{}", price_stat.symbol, price_stat.price_change_percent, price_stat.quote_volume, price_stat.symbol)
// //                 // }
// //                 // history.push(price_points);
// //                 //     for history_point in history.last().unwrap() {
// //                 //         for duration in calc_durations {
// //                 //             let mut duration_last_index = index - duration.as_secs() / check_speed.as_secs();
// //                 //             if duration_last_index < 0 {
// //                 //                 duration_last_index = 0;
// //                 //             }
// //                 //             for history_points in history[duration_last_index as usize ..] {
// //                 //
// //                 //             }
// //                 //
// //                 //         }
// //                 //     }
// //                 // std::thread::sleep(check_speed);
// //                 // index = index + 1;
// //             }
// //
// //         }
// //         _ => {
// //         }
// //     }
// // }
//
//
// use std::str::FromStr;
// use binance::account::{ TimeInForce};
// use binance::futures::account::{CustomOrderRequest,OrderType, FuturesAccount};
// use binance::futures::market::FuturesMarket;
// use binance::futures::model::AccountBalance;
// use binance::model::SymbolPrice;
// use console::Term;
// use crate::Config;
//
// fn get_stop_range(config: &Config, market_price: &SymbolPrice) -> f64 {
// 	return to_precision(market_price.price * (config.stop_loss_percent / 100.0), config.price_precision);
// }
// fn get_target_range(config: &Config, market_price: &SymbolPrice) -> f64 {
// 	return to_precision(market_price.price * (config.target_percent / 100.0), config.price_precision);
// }
// fn get_asset_balance(account: &FuturesAccount, asset: String) -> AccountBalance {
// 	let account_balances: Vec<AccountBalance> = account.account_balance().unwrap().into_iter().filter(|account| account.asset == asset).collect();
// 	let asset_balance = account_balances.get(0).unwrap().clone();
// 	asset_balance
// }
// fn to_precision(num: f64, precision: usize) -> f64 {
// 	f64::from_str(format!("{:.*}", precision, num).as_str()).unwrap()
// }
// fn long(account: &FuturesAccount, market: &FuturesMarket, config: &Config, market_price: &SymbolPrice) {
// 	if config.stop_loss > market_price.price {
// 		println!("Invalid stop");
// 		return
// 	}
// 	let account_balance = get_asset_balance(account, "USDT".to_string());
//
// 	let position_size = to_precision((account_balance.available_balance * config.margin) / market_price.price, config.qty_precision);
// 	println!("Position Size: {} {:?}",  market_price.price, position_size );
//
// 	match account.market_buy(&config.traded_market, position_size) {
// 		Ok(result) => {
// 			let entry_price = account.position_information(&config.traded_market).unwrap().get(0).unwrap().entry_price;
//
// 			let stop_loss = to_precision(config.stop_loss, config.price_precision);
// 			let mut take_profit = to_precision(config.target, config.price_precision);
// 			println!("Stop {} Take {}",stop_loss, take_profit);
// 			let mut stop_order = account.stop_market_close_sell(&config.traded_market, stop_loss).unwrap();
// 			let mut tp_order = account.custom_order(CustomOrderRequest {
// 				symbol: config.traded_market.clone(), side: OrderSide::Sell,
// 				position_side: None,
// 				order_type: OrderType::Limit,
// 				time_in_force: Some(TimeInForce::GTC),
// 				qty: Some(position_size),
// 				reduce_only: Some(true),
// 				price: Some(take_profit),
// 				stop_price: None,
// 				close_position: None,
// 				activation_price: None,
// 				callback_rate: None,
// 				working_type: None,
// 				price_protect: None
// 			}).unwrap();
//
// 			'long_loop: loop {
// 				match listen_for_key() {
// 					's' => {
// 						// market close
// 						account.market_sell(&config.traded_market, position_size);
// 						account.cancel_all_open_orders(&config.traded_market);
// 						break 'long_loop;
// 					}
// 					'r' => {
// 						// reset and return
// 						account.cancel_all_open_orders(&config.traded_market);
// 						break 'long_loop;
// 					}
// 					'b' => {
// 						// breakeven
// 						account.cancel_order(&config.traded_market, stop_order.order_id);
// 						let stop_loss = to_precision(entry_price + ((0.08 * entry_price) / 100_f64), config.price_precision);
//
// 						match account.stop_market_close_sell(&config.traded_market, stop_loss) {
// 							Ok(order) => {stop_order = order}
// 							Err(e) => {
// 								stop_order = account.stop_market_close_sell(&config.traded_market, stop_loss).unwrap();
// 								eprintln!("{}", e)
// 							}
// 						}
//
// 					}
// 					'w' => {
// 						// move target
// 						account.cancel_order(&config.traded_market, tp_order.order_id);
// 						take_profit = to_precision(take_profit + ((0.1 * entry_price) / 100_f64), config.price_precision);
// 						tp_order = account.limit_sell(&config.traded_market, position_size, take_profit, TimeInForce::GTC).unwrap();
// 					}
// 					'q' => {
// 						// move target
// 						account.cancel_order(&config.traded_market, tp_order.order_id);
// 						take_profit = to_precision(take_profit - ((0.1 * entry_price) / 100_f64), config.price_precision);
// 						tp_order = account.limit_sell(&config.traded_market, position_size, take_profit, TimeInForce::GTC).unwrap();
// 					}
// 					'p' => {
// 						let market_price = market.get_price(&config.traded_market).unwrap();
// 						println!(" _____________ {}", take_profit);
// 						println!("|\n|\n|\n|");
// 						if market_price.price > entry_price {
// 							println!("|------------- {}", market_price.price)
// 						}
// 						println!("|\n|");
// 						println!("|------------- {}", entry_price);
// 						println!("|");
// 						if market_price.price < entry_price {
// 							println!("|------------- {}", market_price.price)
// 						}
// 						println!("|");
// 						println!("|------------- {}", stop_loss);
// 					}
// 					_ => {
// 					}
// 				}
// 			}
// 		}
// 		Err(e) => {
// 			eprintln!("{:?}", e)
// 		}
// 	}
// }
//
// fn short(account: &FuturesAccount, market: &FuturesMarket, config: &Config, market_price: &SymbolPrice) {
// 	let market_price = market.get_price(&config.traded_market).unwrap();
// 	if config.stop_loss < market_price.price {
// 		println!("Invalid stop");
// 		return
// 	}
// 	let account_balance = get_asset_balance(account, "USDT".to_string());
// 	let position_size = to_precision((account_balance.available_balance * config.margin) / market_price.price, config.qty_precision);
// 	println!("Position Size: {}", position_size);
// 	match account.market_sell(&config.traded_market, position_size) {
// 		Ok(result) => {
// 			let entry_price = account.position_information(&config.traded_market).unwrap().get(0).unwrap().entry_price;
//
// 			let stop_loss = to_precision(config.stop_loss, config.price_precision);
// 			let mut take_profit = to_precision(config.target, config.price_precision);
// 			println!("Stop: {} Take: {}",stop_loss, take_profit);
// 			let mut stop_order = account.stop_market_close_buy(&config.traded_market, stop_loss).unwrap();
// 			let mut tp_order = account.custom_order(CustomOrderRequest {
// 				symbol: config.traded_market.clone(), side: OrderSide::Buy,
// 				position_side: None,
// 				order_type: OrderType::Limit,
// 				time_in_force: Some(TimeInForce::GTC),
// 				qty: Some(position_size),
// 				reduce_only: Some(true),
// 				price: Some(take_profit),
// 				stop_price: None,
// 				close_position: None,
// 				activation_price: None,
// 				callback_rate: None,
// 				working_type: None,
// 				price_protect: None
// 			}).unwrap();
// 			'short_loop: loop {
// 				match listen_for_key() {
// 					's' => {
// 						// market close
// 						account.market_buy(&config.traded_market, position_size);
// 						account.cancel_all_open_orders(&config.traded_market);
// 						break 'short_loop;
// 					}
// 					'r' => {
// 						// reset and return
// 						account.cancel_all_open_orders(&config.traded_market);
// 						break 'short_loop;
// 					}
// 					'b' => {
// 						// breakeven
// 						account.cancel_order(&config.traded_market, stop_order.order_id);
// 						let stop_loss = to_precision(entry_price - ((0.08 * entry_price) / 100_f64), config.price_precision);
// 						match account.stop_market_close_buy(&config.traded_market, stop_loss) {
// 							Ok(order) => {stop_order = order}
// 							Err(e) => {
// 								eprintln!("{}", e)
// 							}
// 						}
// 					}
// 					'w' => {
// 						// move target
// 						account.cancel_order(&config.traded_market, tp_order.order_id);
// 						take_profit = to_precision(take_profit - ((0.1 * entry_price) / 100_f64), config.price_precision);
// 						tp_order = account.limit_buy(&config.traded_market, position_size, take_profit, TimeInForce::GTC).unwrap();
// 					}
// 					'q' => {
// 						// move target
// 						account.cancel_order(&config.traded_market, tp_order.order_id);
// 						take_profit = to_precision(take_profit + ((0.1 * entry_price) / 100_f64), config.price_precision);
// 						tp_order = account.limit_buy(&config.traded_market, position_size, take_profit, TimeInForce::GTC).unwrap();
// 					}
// 					'p' => {
// 						let market_price = market.get_price(&config.traded_market).unwrap();
// 						println!("|------------- {}", stop_loss);
// 						println!("|");
// 						if market_price.price > entry_price {
// 							println!("|------------- {}", market_price.price)
// 						}
// 						println!("|");
//
// 						println!("|------------- {}", entry_price);
// 						println!("|\n|");
// 						if market_price.price < entry_price {
// 							println!("|------------- {}", market_price.price)
// 						}
// 						println!("|\n|\n|\n|");
// 						println!(" _____________ {}", take_profit);
//
//
//
//
//
// 					}
// 					_ => {
// 					}
// 				}
// 			}
// 		}
// 		Err(e) => {
// 			eprintln!("{:?}", e)
// 		}
// 	}
// }
//
//
// fn listen_for_key() -> char {
// 	let stdout = Term::buffered_stdout();
//
// 	loop {
// 		if let Ok(character) = stdout.read_char() {
// 			return character
// 		}
// 	}
// }