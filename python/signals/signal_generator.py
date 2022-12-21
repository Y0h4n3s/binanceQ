import asyncio
import socket
import sys
import pickle
import os
from cpd import ChangePointDetector
from qpl import QuantumPriceLevels
import asyncclick as click


async def live(grpc_port, symbol):
    sock_path = f"/tmp/live-{symbol}.sock"
    if os.path.exists(sock_path):
        os.remove(sock_path)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(sock_path)
    server.listen(1)
    print(f"[?] signal_generator > {symbol} Waiting for connections on", sock_path)
    connection, client_address = server.accept()
    print(f"[?] signal_generator > {symbol} Listening for klines...")
    cpd = await ChangePointDetector.create("Live", [symbol], grpc_port)
    try:
        while True:
            data = connection.recv(1024)
            if data:
                try:
                    kline = pickle.loads(data)
                    await cpd.live_step(kline["symbol"], kline)
                except pickle.UnpicklingError as e:
                    pass
                finally:
                    connection.send(pickle.dumps({"msg": "OK"}))

    finally:
        connection.close()


async def backtest(grpc_port, symbol):
    sock_path = f"/tmp/backtest-{symbol}.sock"
    if os.path.exists(sock_path):
        os.remove(sock_path)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(sock_path)
    server.listen(1)
    print(f"[?] signal_generator > {symbol} Waiting for connections on", sock_path)
    connection, client_address = server.accept()
    print(f"[?] signal_generator > {symbol} Listening for klines...")
    cpd = await ChangePointDetector.create("Backtest", [symbol], grpc_port)
    try:
        while True:
            data = connection.recv(1024)
            if data:
                try:
                    kline = pickle.loads(data)
                    await cpd.backtest_step(kline["symbol"], kline)
                except pickle.UnpicklingError as e:
                    pass
                finally:
                    connection.send(pickle.dumps({"msg": "OK"}))

    finally:
        connection.close()


@click.command()
@click.option("--grpc-port", default=50011, help="Grpc port to send signals and orders")
@click.option("--symbol", default="BTCUSDT")
@click.option("--mode", default="Backtest")
async def main(grpc_port, symbol, mode):
    if mode == "Live":
        await live(grpc_port, symbol)
    if mode == "Backtest":
        await backtest(grpc_port, symbol)


if __name__ == "__main__":
    main(_anyio_backend="asyncio")