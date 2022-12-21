import numpy as np
import plotly.express as px
import datetime
from sdt.changepoint import BayesOnline
import time
from sklearn.preprocessing import StandardScaler
from binance import AsyncClient, BinanceSocketManager
import grpc
import signals_pb2_grpc as signals_pb2_grpc
import signals_pb2
import uuid
from qpl import QuantumPriceLevels


class ChangePointDetector:

    def __init__(self, symbols, min_buffer, max_buffer, tf, grpc_server):
        assert min_buffer <= max_buffer
        self.detectors = {}
        self.steps = 0
        self.symbols = symbols
        self.min_buffer = min_buffer
        self.max_buffer = max_buffer
        self.grpc_server = grpc_server
        self.tf = tf
        self.scaler = StandardScaler()

    @classmethod
    async def create(cls, mode, symbols, grpc_port, min_buffer=300, max_buffer=3000,
                     tf=AsyncClient.KLINE_INTERVAL_15MINUTE,
                     grpc_server="localhost:50051"):
        self = cls(symbols, min_buffer, max_buffer, tf, grpc_server)
        self.qpl = await QuantumPriceLevels.create(symbols)
        self.client = await AsyncClient.create()
        self.grpc_server = f"localhost:{grpc_port}"
        for symbol in symbols:
            my_detector = BayesOnline()
            values = np.array([])

            if mode == "Live":
                # initialize the detector with the last min_buffer values
                trades = await self.client.get_historical_klines(symbol, tf, limit=min_buffer)

                values = []
                for i in range(0, len(trades)):
                    self.qpl.step({"symbol": symbol}, {"close": trades[i][2], "open": trades[i][1]})
                    o = float(trades[i][1])
                    c = float(trades[i][2])
                    h = float(trades[i][3])
                    l = float(trades[i][4])
                    if o > c:
                        u_percent = h - o / 1 if np.abs(c - o) else np.abs(c - o)
                        if len(values) == 0:
                            values.append(np.abs(c - o) * 1 if u_percent == 0 else np.abs(u_percent))
                        else:
                            values.append(
                                np.abs(values[-1] + np.abs(c - o) * 1 if u_percent == 0 else np.abs(u_percent)) * -1)
                    else:
                        l_percent = o - l / 1 if np.abs(c - o) else np.abs(c - o)
                        if len(values) == 0:
                            values.append(np.abs(c - o) * 1 if l_percent == 0 else np.abs(l_percent))
                        else:
                            values.append(
                                np.abs(values[-1]) + np.abs(c - o) * 1 if l_percent == 0 else np.abs(l_percent))

                values = np.array(values)
                scaled = self.scaler.fit_transform(values.reshape(-1, 1), sample_weight=100)
                my_detector.find_changepoints(scaled, past=10)
                self.steps += min_buffer

            self.detectors[symbol] = {
                "detector": my_detector,
                "vals": values
            }

        return self

    def moving_average(self, x, w):
        return np.convolve(x, np.ones(w), 'valid') / w

    async def backtest_step(self, s, kline):
        pqpl, nqpl = self.qpl.step(s, kline)
        symbol = s["symbol"]
        self.steps += 1
        # update the detector
        m_detector = self.detectors[symbol]['detector']
        vals = self.detectors[symbol]["vals"].tolist()
        o1 = float(kline["open"])
        c1 = float(kline["close"])
        h1 = float(kline["high"])
        l1 = float(kline["low"])
        t = float(kline["close_time"])
        if o1 > c1:
            u_percent = h1 - o1 / 1 if np.abs(c1 - o1) else np.abs(c1 - o1)
            l_percent = c1 - l1 / 1 if np.abs(c1 - o1) else np.abs(c1 - o1)
            if len(vals) == 0:
                vals.append(np.abs(c1 - o1) * 1 if u_percent == 0 else np.abs(u_percent))
            else:
                vals.append(np.abs(vals[-1] + np.abs(c1 - o1) * 1 if u_percent == 0 else np.abs(u_percent)) * -1)
        else:
            u_percent = h1 - c1 / 1 if np.abs(c1 - o1) else np.abs(c1 - o1)
            l_percent = o1 - l1 / 1 if np.abs(c1 - o1) else np.abs(c1 - o1)
            if len(vals) == 0:
                vals.append(np.abs(c1 - o1) * 1 if l_percent == 0 else np.abs(l_percent))
            else:
                vals.append(np.abs(vals[-1]) + np.abs(c1 - o1) * 1 if l_percent == 0 else np.abs(l_percent))

        vals = np.array(vals)

        if len(vals) < 14:
            self.detectors[symbol]["vals"] = vals
            return
        scaled1 = self.scaler.fit_transform(vals.reshape(-1, 1), sample_weight=100)
        diff = []
        for i in range(1, len(scaled1)):
            diff.append(np.abs(scaled1[i] - scaled1[i - 1])[0])
        adiff = self.moving_average(np.array(diff), 14)
        scaled_ma = self.moving_average(scaled1.flatten(), 13)
        m_detector.update(scaled_ma[-1])

        self.detectors[symbol]['detector'] = m_detector
        self.detectors[symbol]["vals"] = vals

        if self.steps < self.min_buffer:
            return

        # decide if we should buy or sell or do nothing
        # then notify rust code via RPC
        probs = m_detector.get_probabilities(5)
        average = np.mean(probs)
        # print(f"Average probability: {average}")

        # TODO: Send cancel order if recent open order is not going as expected
        # TODO: expose exchange account info via RPC
        if probs[-1] > average * 10 and len(pqpl) > 0:
            with grpc.insecure_channel(self.grpc_server) as channel:
                print(self.steps)
                stub = signals_pb2_grpc.SignalGeneratorStub(channel)
                s["exchange"] = signals_pb2.ExchangeId.EXCHANGE_ID_SIMULATED

                position = stub.GetPosition(signals_pb2.Symbol(**s))
                spread = stub.GetSpread(signals_pb2.Symbol(**s))
                pqpl, nqpl = self.qpl.qpl_at_price(symbol, spread.spread)

                print(position)
                lifetime = 3000 * 60 * 1000
                entry_id = str(uuid.uuid4())
                quantity = round(100 / spread.spread, 5)
                entry_order = {
                    "id": entry_id,
                    "symbol": signals_pb2.Symbol(**s),
                    "side": signals_pb2.Side.SIDE_BUY,
                    "price": spread.spread,
                    "quantity": quantity,
                    "order_type": signals_pb2.OrderType.ORDER_TYPE_MARKET,
                    "lifetime": lifetime,
                    "timestamp": int(t),
                    "close_policy": signals_pb2.ClosePolicy.CLOSE_POLICY_IMMEDIATE_MARKET,
                    "for_id": entry_id,
                    "extra": ""
                }
                stop_loss_order = {
                    "id": str(uuid.uuid4()),
                    "symbol": signals_pb2.Symbol(**s),
                    "side": signals_pb2.Side.SIDE_SELL,
                    "price": 0,
                    "quantity": quantity,
                    "order_type": signals_pb2.OrderType.ORDER_TYPE_STOP_LOSS,
                    "lifetime": lifetime,
                    "timestamp": int(t),
                    "close_policy": signals_pb2.ClosePolicy.CLOSE_POLICY_IMMEDIATE_MARKET,
                    "for_id": entry_id,
                    "extra": ""

                }
                take_profit_order = {
                    "id": str(uuid.uuid4()),
                    "symbol": signals_pb2.Symbol(**s),
                    "side": signals_pb2.Side.SIDE_SELL,
                    "price": 0,
                    "quantity": quantity,
                    "order_type": signals_pb2.OrderType.ORDER_TYPE_TAKE_PROFIT,
                    "lifetime": lifetime,
                    "timestamp": int(t),
                    "close_policy": signals_pb2.ClosePolicy.CLOSE_POLICY_IMMEDIATE_MARKET,
                    "for_id": entry_id,
                    "extra": ""

                }
                orders = []
                if scaled_ma[-1] > 0:
                    target = pqpl[3]
                    stop_loss = nqpl[1]
                    stop_loss_order["price"] = stop_loss
                    stop_loss_order["extra"] = str(spread.spread - stop_loss)
                    take_profit_order["price"] = target

                    if position.qty == 0:
                        orders.append(signals_pb2.Order(**entry_order))
                        orders.append(signals_pb2.Order(**stop_loss_order))
                        orders.append(signals_pb2.Order(**take_profit_order))
                        print(f"[+] BUY {symbol} {spread.spread} {target} {stop_loss} {probs[-1]}")
                    else:
                        if position.side == signals_pb2.Side.SIDE_BUY:
                            orders.append(signals_pb2.Order(**entry_order))
                            orders.append(signals_pb2.Order(**take_profit_order))
                            orders.append(signals_pb2.Order(**stop_loss_order))

                        else:
                            closing_order = {
                                "id": str(uuid.uuid4()),
                                "symbol": signals_pb2.Symbol(**s),
                                "side": signals_pb2.Side.SIDE_BUY,
                                "price": spread.spread,
                                "quantity": position.qty,
                                "order_type": signals_pb2.OrderType.ORDER_TYPE_MARKET,
                                "lifetime": lifetime,
                                "timestamp": int(t),
                                "close_policy": signals_pb2.ClosePolicy.CLOSE_POLICY_IMMEDIATE_MARKET,
                                "for_id": entry_id,
                                "extra": ""
                            }
                            orders.append(signals_pb2.Order(**closing_order))
                            orders.append(signals_pb2.Order(**entry_order))
                            orders.append(signals_pb2.Order(**stop_loss_order))
                            orders.append(signals_pb2.Order(**take_profit_order))
                        print(f"[+] BUY {symbol} {spread.spread} {target} {stop_loss} {probs[-1]}")
                else:
                    entry_order["side"] = signals_pb2.Side.SIDE_SELL
                    stop_loss_order["side"] = signals_pb2.Side.SIDE_BUY
                    take_profit_order["side"] = signals_pb2.Side.SIDE_BUY
                    stop_loss = pqpl[1]
                    target = nqpl[3]
                    stop_loss_order["price"] = stop_loss
                    stop_loss_order["extra"] = str(stop_loss - spread.spread)
                    take_profit_order["price"] = target

                    if position.qty == 0:
                        orders.append(signals_pb2.Order(**entry_order))
                        orders.append(signals_pb2.Order(**stop_loss_order))
                        orders.append(signals_pb2.Order(**take_profit_order))
                        print(f"[+] SELL {symbol} {spread.spread} {target} {stop_loss} {probs[-1]}")
                    else:
                        if position.side == signals_pb2.Side.SIDE_SELL:
                            orders.append(signals_pb2.Order(**entry_order))

                            orders.append(signals_pb2.Order(**take_profit_order))
                            orders.append(signals_pb2.Order(**stop_loss_order))

                        else:
                            closing_order = {
                                "id": str(uuid.uuid4()),
                                "symbol": signals_pb2.Symbol(**s),
                                "side": signals_pb2.Side.SIDE_SELL,
                                "price": spread.spread,
                                "quantity": position.qty,
                                "order_type": signals_pb2.OrderType.ORDER_TYPE_MARKET,
                                "lifetime": lifetime,
                                "timestamp": int(t),
                                "close_policy": signals_pb2.ClosePolicy.CLOSE_POLICY_IMMEDIATE_MARKET,
                                "for_id": entry_id,
                                "extra": ""
                            }
                            orders.append(signals_pb2.Order(**closing_order))
                            orders.append(signals_pb2.Order(**entry_order))
                            orders.append(signals_pb2.Order(**stop_loss_order))
                            orders.append(signals_pb2.Order(**take_profit_order))
                        print(f"[+] SELL {symbol} {spread.spread} {target} {stop_loss} {probs[-1]}")
                resp = stub.NotifyOrders(signals_pb2.Orders(orders=orders))

        # reset to min_buffer if we have too many values
        if self.steps > self.max_buffer:
            vals = self.detectors[symbol]["vals"][-self.min_buffer:]
            detector = BayesOnline()
            scaled = self.scaler.fit_transform(vals.reshape(-1, 1), sample_weight=100)
            detector.find_changepoints(scaled)
            self.detectors[symbol]['detector'] = detector
            self.detectors[symbol]["vals"] = vals
            self.steps = self.min_buffer

    async def live_step(self, s, kline):
        pqpl, nqpl = self.qpl.step(s, kline)
        symbol = s["symbol"]
        self.steps += 1
        # update the detector
        m_detector = self.detectors[symbol]['detector']
        vals = self.detectors[symbol]["vals"].tolist()
        o1 = float(kline["open"])
        c1 = float(kline["close"])
        h1 = float(kline["high"])
        l1 = float(kline["low"])
        t = float(kline["close_time"])
        if o1 > c1:
            u_percent = h1 - o1 / 1 if np.abs(c1 - o1) else np.abs(c1 - o1)
            if len(vals) == 0:
                vals.append(np.abs(c1 - o1) * 1 if u_percent == 0 else np.abs(u_percent))
            else:
                vals.append(np.abs(vals[-1] + np.abs(c1 - o1) * 1 if u_percent == 0 else np.abs(u_percent)) * -1)
        else:
            l_percent = o1 - l1 / 1 if np.abs(c1 - o1) else np.abs(c1 - o1)
            if len(vals) == 0:
                vals.append(np.abs(c1 - o1) * 1 if l_percent == 0 else np.abs(l_percent))
            else:
                vals.append(np.abs(vals[-1]) + np.abs(c1 - o1) * 1 if l_percent == 0 else np.abs(l_percent))

        vals = np.array(vals)

        if len(vals) < 14:
            self.detectors[symbol]["vals"] = vals
            return
        scaled1 = self.scaler.fit_transform(vals.reshape(-1, 1), sample_weight=100)
        diff = []
        for i in range(1, len(scaled1)):
            diff.append(np.abs(scaled1[i] - scaled1[i - 1])[0])
        scaled_ma = self.moving_average(scaled1.flatten(), 13)
        m_detector.update(scaled_ma[-1])

        self.detectors[symbol]['detector'] = m_detector
        self.detectors[symbol]["vals"] = vals

        if self.steps < self.min_buffer:
            return

        # decide if we should buy or sell or do nothing
        # then notify rust code via RPC
        probs = m_detector.get_probabilities(5)
        average = np.mean(probs)
        # print(f"Average probability: {average}")

        # TODO: Send cancel order if recent open order is not going as expected

        print(f"[?] CPD > {symbol} Average Probability: {average} Current: {probs[-1]}")
        if probs[-1] > average * 10 and len(pqpl) > 0:
            with grpc.insecure_channel(self.grpc_server) as channel:
                stub = signals_pb2_grpc.SignalGeneratorStub(channel)
                s["exchange"] = signals_pb2.ExchangeId.EXCHANGE_ID_BINANCE

                position = stub.GetPosition(signals_pb2.Symbol(**s))
                spread = stub.GetSpread(signals_pb2.Symbol(**s))
                pqpl, nqpl = self.qpl.qpl_at_price(symbol, spread.spread)

                print(f"[?] CPD > Current Position {position}")
                print(f"[?] CPD > Current Spread {spread}")
                lifetime = 3000 * 60 * 1000
                entry_id = str(uuid.uuid4())
                quantity = round(10 / spread.spread, s["base_asset_precision"])
                entry_order = {
                    "id": entry_id,
                    "symbol": signals_pb2.Symbol(**s),
                    "side": signals_pb2.Side.SIDE_BUY,
                    "price": round(spread.spread, s["quote_asset_precision"]),
                    "quantity": quantity,
                    "order_type": signals_pb2.OrderType.ORDER_TYPE_MARKET,
                    "lifetime": lifetime,
                    "timestamp": int(t),
                    "close_policy": signals_pb2.ClosePolicy.CLOSE_POLICY_IMMEDIATE_MARKET,
                    "for_id": entry_id,
                    "extra": ""
                }
                stop_loss_order = {
                    "id": str(uuid.uuid4()),
                    "symbol": signals_pb2.Symbol(**s),
                    "side": signals_pb2.Side.SIDE_SELL,
                    "price": 0,
                    "quantity": quantity,
                    "order_type": signals_pb2.OrderType.ORDER_TYPE_STOP_LOSS,
                    "lifetime": lifetime,
                    "timestamp": int(t),
                    "close_policy": signals_pb2.ClosePolicy.CLOSE_POLICY_IMMEDIATE_MARKET,
                    "for_id": entry_id,
                    "extra": ""

                }
                take_profit_order = {
                    "id": str(uuid.uuid4()),
                    "symbol": signals_pb2.Symbol(**s),
                    "side": signals_pb2.Side.SIDE_SELL,
                    "price": 0,
                    "quantity": quantity,
                    "order_type": signals_pb2.OrderType.ORDER_TYPE_TAKE_PROFIT,
                    "lifetime": lifetime,
                    "timestamp": int(t),
                    "close_policy": signals_pb2.ClosePolicy.CLOSE_POLICY_IMMEDIATE_MARKET,
                    "for_id": entry_id,
                    "extra": ""

                }
                orders = []
                if scaled_ma[-1] > 0:
                    target = round(pqpl[0], s["quote_asset_precision"])
                    stop_loss = round(nqpl[0], s["quote_asset_precision"])
                    stop_loss_order["price"] = stop_loss
                    stop_loss_order["extra"] = str(spread.spread - stop_loss)
                    take_profit_order["price"] = target

                    if position.qty == 0:
                        orders.append(signals_pb2.Order(**entry_order))
                        orders.append(signals_pb2.Order(**stop_loss_order))
                        orders.append(signals_pb2.Order(**take_profit_order))
                        print(f"[+] BUY {symbol} {spread.spread} {target} {stop_loss} {probs[-1]}")
                    else:
                        if position.side == signals_pb2.Side.SIDE_BUY:
                            orders.append(signals_pb2.Order(**entry_order))
                            orders.append(signals_pb2.Order(**take_profit_order))
                            orders.append(signals_pb2.Order(**stop_loss_order))

                        else:
                            closing_order = {
                                "id": str(uuid.uuid4()),
                                "symbol": signals_pb2.Symbol(**s),
                                "side": signals_pb2.Side.SIDE_BUY,
                                "price": spread.spread,
                                "quantity": position.qty,
                                "order_type": signals_pb2.OrderType.ORDER_TYPE_MARKET,
                                "lifetime": lifetime,
                                "timestamp": int(t),
                                "close_policy": signals_pb2.ClosePolicy.CLOSE_POLICY_IMMEDIATE_MARKET,
                                "for_id": entry_id,
                                "extra": ""
                            }
                            orders.append(signals_pb2.Order(**closing_order))
                            orders.append(signals_pb2.Order(**entry_order))
                            orders.append(signals_pb2.Order(**stop_loss_order))
                            orders.append(signals_pb2.Order(**take_profit_order))
                        print(f"[+] BUY {symbol} {spread.spread} {target} {stop_loss} {probs[-1]}")
                else:
                    entry_order["side"] = signals_pb2.Side.SIDE_SELL
                    stop_loss_order["side"] = signals_pb2.Side.SIDE_BUY
                    take_profit_order["side"] = signals_pb2.Side.SIDE_BUY
                    stop_loss = round(pqpl[0], s["quote_asset_precision"])
                    target = round(nqpl[0], s["quote_asset_precision"])
                    stop_loss_order["price"] = stop_loss
                    stop_loss_order["extra"] = str(stop_loss - spread.spread)
                    take_profit_order["price"] = target

                    if position.qty == 0:
                        orders.append(signals_pb2.Order(**entry_order))
                        orders.append(signals_pb2.Order(**stop_loss_order))
                        orders.append(signals_pb2.Order(**take_profit_order))
                        print(f"[+] SELL {symbol} {spread.spread} {target} {stop_loss} {probs[-1]}")
                    else:
                        if position.side == signals_pb2.Side.SIDE_SELL:
                            orders.append(signals_pb2.Order(**entry_order))

                            orders.append(signals_pb2.Order(**take_profit_order))
                            orders.append(signals_pb2.Order(**stop_loss_order))

                        else:
                            closing_order = {
                                "id": str(uuid.uuid4()),
                                "symbol": signals_pb2.Symbol(**s),
                                "side": signals_pb2.Side.SIDE_SELL,
                                "price": spread.spread,
                                "quantity": position.qty,
                                "order_type": signals_pb2.OrderType.ORDER_TYPE_MARKET,
                                "lifetime": lifetime,
                                "timestamp": int(t),
                                "close_policy": signals_pb2.ClosePolicy.CLOSE_POLICY_IMMEDIATE_MARKET,
                                "for_id": entry_id,
                                "extra": ""
                            }
                            orders.append(signals_pb2.Order(**closing_order))
                            orders.append(signals_pb2.Order(**entry_order))
                            orders.append(signals_pb2.Order(**stop_loss_order))
                            orders.append(signals_pb2.Order(**take_profit_order))
                        print(f"[+] SELL {symbol} {spread.spread} {target} {stop_loss} {probs[-1]}")
                resp = stub.NotifyOrders(signals_pb2.Orders(orders=orders))
                print(resp)

        # reset to min_buffer if we have too many values
        if self.steps > self.max_buffer:
            vals = self.detectors[symbol]["vals"][-self.min_buffer:]
            detector = BayesOnline()
            scaled = self.scaler.fit_transform(vals.reshape(-1, 1), sample_weight=100)
            scaled_ma = self.moving_average(scaled.flatten(), 13)

            detector.find_changepoints(scaled_ma)
            self.detectors[symbol]['detector'] = detector
            self.detectors[symbol]["vals"] = vals
            self.steps = self.min_buffer


async def live():
    symbols = ["XRPUSDT", "DOGEUSDT", "APEUSDT", "ETHUSDT", "SOLUSDT"]
    client = await AsyncClient.create("ftcpi3OSjk26htxak54hqkZ6e9vdHq2Vd7oN83VZN39UcYmw1VwVkibug52oGIs4",
                                      "iXArQEDFfmFanIfz7RfJj6G034b76nDuNetqxdqSLiUwPqDtLGIaNYK4TgDxR9H4")
    bm = BinanceSocketManager(client)
    scaler = StandardScaler()
    detectors = {}
    for symbol in symbols:
        # open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore
        trades = await client.get_historical_klines(symbol, AsyncClient.KLINE_INTERVAL_1MINUTE, "4 hours ago UTC")
        values = []
        for i in range(0, len(trades)):
            o = float(trades[i][1])
            c = float(trades[i][2])
            h = float(trades[i][3])
            l = float(trades[i][4])
            if o > c:
                values.append(np.log((c + l) / (o + h)) * max(h - l, 1))
            else:
                values.append(np.log((c + h) / (o + l)) * max(h - l, 1))

        values = np.array(values)
        scaled = scaler.fit_transform(values.reshape(-1, 1), sample_weight=100)
        my_detector = BayesOnline()
        my_detector.find_changepoints(scaled, past=10)
        detectors[symbol] = {
            "detector": my_detector,
            "vals": values
        }

    def handle_kline_message(msg):
        symbol1 = msg['stream'].split("@")[0].upper()
        m_detector = detectors[symbol1]['detector']
        vals = detectors[symbol1]["vals"].tolist()
        o1 = float(msg["data"]['k']["o"])
        c1 = float(msg["data"]['k']["c"])
        h1 = float(msg["data"]['k']["h"])
        l1 = float(msg["data"]['k']["l"])
        if o1 > c1:
            vals.append(np.log((c1 + l1) / (o1 + h1)) * max(h1 - l1, 1))
        else:
            vals.append(np.log((c1 + h1) / (o1 + l1)) * max(h1 - l1, 1))

        vals = np.array(vals)
        scaled1 = scaler.fit_transform(vals.reshape(-1, 1), sample_weight=100)
        m_detector.update(scaled1[-1])
        prob = m_detector.get_probabilities(1)[-1]
        if prob > 0.1:
            print(symbol1, ": ", prob)
            fig = px.line(m_detector.get_probabilities(1)[5:])
            fig.show()
        detectors[symbol1]['detector'] = m_detector
        detectors[symbol1]["vals"] = vals

    multiplex = []
    for symbol in symbols:
        multiplex.append(symbol.lower() + "@" + "kline_1m")
    print(multiplex)
    ks = bm.multiplex_socket(multiplex)
    print("Listening for candle closes...")

    last_close = trades[-1][6]
    print(datetime.datetime.fromtimestamp(float(last_close) / 1000))

    # async with ks as tscm:
    #     while True:
    #         res = await tscm.recv()
    #         handle_kline_message(res)

    def update_detector(sym, t):
        m_detector = detectors[sym]['detector']
        vals = detectors[sym]["vals"].tolist()
        o1 = float(t[1])
        c1 = float(t[2])
        h1 = float(t[3])
        l1 = float(t[4])
        if o1 > c1:
            vals.append(np.log((c1 + l1) / (o1 + h1)) * max(h1 - l1, 1))
        else:
            vals.append(np.log((c1 + h1) / (o1 + l1)) * max(h1 - l1, 1))

        vals = np.array(vals)
        scaled1 = scaler.fit_transform(vals.reshape(-1, 1), sample_weight=100)
        m_detector.update(scaled1[-1])
        prob = m_detector.get_probabilities(1)[-1]
        if prob > 0.1:
            print(sym, ": ", prob, o1, c1, h1, l1)
            fig = px.line(m_detector.get_probabilities(1)[5:])
            fig.show()
        detectors[sym]['detector'] = m_detector
        detectors[sym]["vals"] = vals

    while True:
        tf = last_close
        for symbol in symbols:
            trade = await client.get_klines(symbol=symbol, interval=AsyncClient.KLINE_INTERVAL_1MINUTE, limit=2)
            if trade[0][6] > last_close:
                tf = trade[0][6]
                print(datetime.datetime.fromtimestamp(float(tf) / 1000))
                update_detector(symbol, trade[0])
        time.sleep(5)
        last_close = tf
