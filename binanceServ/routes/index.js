var express = require('express');
var router = express.Router();
const Binance = require('binance-api-node').default
const MARKET = "BTCUSDT";

const binance = Binance({
  apiKey: 'ftcpi3OSjk26htxak54hqkZ6e9vdHq2Vd7oN83VZN39UcYmw1VwVkibug52oGIs4',
  apiSecret: 'iXArQEDFfmFanIfz7RfJj6G034b76nDuNetqxdqSLiUwPqDtLGIaNYK4TgDxR9H4',
})
/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

router.post("/open", async function(req, res, next) {
  let order = req.body;
  console.log(order)
  let orders = await binance.futuresOpenOrders({symbol: MARKET})
  if (orders.length > 1) return
  if (orders.length === 1) {
    await binance.futuresCancelAllOpenOrders({symbol: MARKET})
  }
  if (order.side === "long") {
    binance.futuresOrder({
      side: "BUY",
      type: "MARKET",
      quantity: "0.01",
      symbol: MARKET
    }).then(r => {
      console.log(r)
      binance.futuresOrder({
        side: "SELL",
        type: "STOP_MARKET",
        stopPrice: order.stop.toFixed(1),
        quantity: "0.01",
        symbol: MARKET,
        reduceOnly: "true"

      }).then(async re => {
        let prices = await binance.futuresPrices();

        binance.futuresOrder({
          side: "SELL",
          type: "LIMIT",
          reduceOnly: "true",
          price: (Number(prices[MARKET]) + order.profit / 10).toFixed(1),
          quantity: "0.01",
          symbol: MARKET

        })
      })
    })
  }
  else if (order.side === "short") {
    binance.futuresOrder({
      side: "SELL",
      type: "MARKET",
      quantity: "0.01",
      symbol: MARKET
    }).then(r => {
      console.log(r)
      binance.futuresOrder({
        side: "BUY",
        type: "STOP_MARKET",
        stopPrice: order.stop.toFixed(1),
        quantity: "0.01",
        symbol: MARKET,
        reduceOnly: "true"

      }).then(async re => {
        let prices = await binance.futuresPrices();

        binance.futuresOrder({
          side: "BUY",
          type: "LIMIT",
          price: (Number(prices[MARKET]) - order.profit / 10).toFixed(1),
          quantity: "0.01",
          symbol: MARKET,
          reduceOnly: "true"

        })
      })
    })
  }
})
module.exports = router;
