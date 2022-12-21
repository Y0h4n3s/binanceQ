from sklearn.preprocessing import StandardScaler
import numpy as np


def ret_wave_span(r, returns, mu, r0):
    rets = 0
    if r > r0:
        rets = np.count_nonzero((returns <= r) & (returns > r - mu))

    else:
        rets = np.count_nonzero((returns > r) & (returns <= r + mu))

    return rets / len(returns), rets


def ret_wave(r, returns):
    return np.count_nonzero(returns == r) / len(returns)  # np.count_nonzero((returns >= 0) & (returns <= 1))


def qpl(lam, sigma, price):
    max_n = 21;  # Max Quantum Price Energy Levels

    qpe = []
    qpr = []
    nqpr = []
    nqpl = []
    pqpl = []

    for n in range(0, max_n):
        k = ((1.1924 + 33.232383 * n + 56.22169 * n * n) / (1 + 43.6106 * n))
        p = -1 * pow((2 * n + 1), 2)
        q = -1 * lam * pow((2 * n + 1), 3) * pow(k, 3)

        u = pow((-0.5 * q + np.sqrt(((q * q / 4.0) + (p * p * p / 27.0)))), 1 / 3)
        v = pow((-0.5 * q - np.sqrt(((q * q / 4.0) + (p * p * p / 27.0)))), 1 / 3)

        qpe.append(u + v)

    for n in range(1, max_n):
        qpr.append(qpe[n - 1] / qpe[0])
        nqpr.append(1 + 0.21 * sigma * qpr[n - 1])
    for n in range(1, max_n - 1):
        pqpl.append(price * nqpr[n])
        nqpl.append(price / nqpr[n])
    return pqpl, nqpl


class QuantumPriceLevels:

    def __init__(self, symbols, min_buffer, max_buffer):
        assert min_buffer <= max_buffer
        self.data = {}
        self.steps = 0
        self.symbols = symbols
        self.min_buffer = min_buffer
        self.max_buffer = max_buffer
        self.scaler = StandardScaler()

    @classmethod
    async def create(cls, symbols, min_buffer=200, max_buffer=10000):
        self = cls(symbols, min_buffer, max_buffer)
        for symbol in symbols:
            self.data[symbol] = {
                "Open": [], "High": [], "Low": [], "Close": [], "Volume": [], "Return": [], "Wave": []
            }
        return self

    def step(self, s, kline):
        symbol = s["symbol"]
        self.steps += 1
        data = self.data[symbol]
        # data["Open"].append(float(kline["open"]))
        # data["High"].append(float(kline["high"]))
        # data["Low"].append(float(kline["low"]))
        # data["Close"].append(float(kline["close"]))
        # data["Volume"].append(float(kline["volume"]))
        ret = round(float(kline["close"]) / float(kline["open"]),5)
        data["Return"].append(ret)



        wave = []
        rets = np.array(data["Return"])
        for r in rets:
            wave.append(ret_wave(r, rets))

        data["Wave"] = wave
        if self.steps < self.min_buffer:
            return [], []
        sd = np.std(rets)
        sigma = np.std(wave)
        mu = (sd * 3) / 50
        r0 = 1 - mu / 2
        rp1 = r0 + mu
        rm1 = r0 - mu
        wrp1, _ = ret_wave_span(rp1, rets, mu, r0)
        wrm1, _ = ret_wave_span(rm1, rets, mu, r0)

        lam = np.absolute(((np.power(rm1, 2) * wrm1) - (np.power(rp1, 2) * wrp1)) / (
                    (np.power(rp1, 4) * wrp1) - (np.power(rm1, 4) * wrm1)))

        if self.steps > self.max_buffer:
            self.data["Return"] = data["Return"][-self.min_buffer:]

        return qpl(lam, sigma, float(kline["close"]))

    def qpl_at_price(self, symbol, price):
        data = self.data[symbol]
        rets = np.array(data["Return"])
        wave = np.array(data["Wave"])
        sd = np.std(rets)
        sigma = np.std(wave)
        mu = (sd * 3) / 50
        r0 = 1 - mu / 2
        rp1 = r0 + mu
        rm1 = r0 - mu
        wrp1, _ = ret_wave_span(rp1, rets, mu, r0)
        wrm1, _ = ret_wave_span(rm1, rets, mu, r0)

        lam = np.absolute(((np.power(rm1, 2) * wrm1) - (np.power(rp1, 2) * wrp1)) / (
                (np.power(rp1, 4) * wrp1) - (np.power(rm1, 4) * wrm1)))
        return qpl(lam, sigma, price)
