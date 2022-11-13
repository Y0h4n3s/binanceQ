
use binance::account::TimeInForce;
use binance::api::Binance;
use binance::futures::account::FuturesAccount;
use binance::futures::market::FuturesMarket;
use serde::{Serialize, Deserialize};
use crate::MARKET;

#[derive(Debug, Deserialize, Serialize)]
pub struct OpenRequest {
	pub side: String,
	pub stop: f64,
	pub profit: u64,
	pub msg: String
}
//
// #[post("/open")]
// pub async fn create(order_req: Json<OpenRequest>) -> HttpResponse {
// 	let api_key = Some("ftcpi3OSjk26htxak54hqkZ6e9vdHq2Vd7oN83VZN39UcYmw1VwVkibug52oGIs4".into());
//     let secret_key = Some("iXArQEDFfmFanIfz7RfJj6G034b76nDuNetqxdqSLiUwPqDtLGIaNYK4TgDxR9H4".into());
//     let market: FuturesMarket = Binance::new(api_key.clone(), secret_key.clone());
//     let account: FuturesAccount = Binance::new(api_key.clone(), secret_key.clone());
// 	if &order_req.side == "long" {
// 		match account.market_buy(MARKET, 0.05) {
// 			Ok(result) => {
//
// 				let mut stop_order = account.stop_market_close_sell(MARKET, order_req.stop.floor()).unwrap();
// 			}
// 			Err(e) => {
// 				eprintln!("{:?}", e)
// 			}
// 		}
// 	} else {
// 		match account.market_sell(MARKET, 0.05) {
// 			Ok(result) => {
//
// 				let mut stop_order = account.stop_market_close_buy(MARKET, order_req.stop.floor()).unwrap();
// 			}
// 			Err(e) => {
// 				eprintln!("{:?}", e)
// 			}
// 		}
// 	}
//
// 	HttpResponse::Created()
// 		  .json(order_req)
// }