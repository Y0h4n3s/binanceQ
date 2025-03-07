import sqlite3
import pandas as pd
import argparse
import plotly.graph_objects as go
from plotly.basedatatypes import BaseFigure
from plotly.subplots import make_subplots
import datetime
import matplotlib.pyplot as plt

from sympy.physics.units import hours


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Visualize backtest trades.')
    parser.add_argument('symbol', type=str, help='Symbol to filter data')
    parser.add_argument('trades', type=int, help='Number of trades to display')
    parser.add_argument('tf', type=str, help='timeframe for charts')
    args = parser.parse_args()

    symbol = args.symbol
    tf = args.tf
    num_trades = args.trades

    # Connect to the SQLite database
    conn = sqlite3.connect('binance_studies.db')  # Replace with your database file
    print("Connected to the database.")

    # Load data into DataFrames
    trades_df = pd.read_sql_query("SELECT * FROM trades WHERE symbol = ?", conn, params=(symbol,))
    klines_df = pd.read_sql_query("SELECT * FROM tf_trades WHERE symbol = ? AND tf = ?", conn, params=(symbol,tf))
    orders_df = pd.read_sql_query("SELECT * FROM orders WHERE symbol = ?", conn, params=(symbol,))
    conn.close()
    print(f"Loaded data for symbol: {symbol}")

    trades_df['trade_time'] = pd.to_datetime(trades_df['time'], unit='ms')
    orders_df['order_time'] = pd.to_datetime(orders_df['time'], unit='ms')
    klines_df['open_time'] = pd.to_datetime(klines_df['open_time'], unit='ms')
    klines_df['close_time'] = pd.to_datetime(klines_df['close_time'], unit='ms')
    # Ensure correct data types
    trades_df['realized_pnl'] = trades_df['realized_pnl'].astype(float)
    trades_df['price'] = trades_df['price'].astype(float)
    trades_df['qty'] = trades_df['qty'].astype(float)
    trades_df['side'] = trades_df['side'].astype(str)

    orders_df['price'] = orders_df['price'].astype(float)
    orders_df['quantity'] = orders_df['quantity'].astype(float)
    orders_df['side'] = orders_df['side'].astype(str)

    # Estimate entry prices
    trades_df['entry_price'] = trades_df.apply(estimate_entry_price, axis=1)
    trades_df['cumulative_pnl'] = trades_df['realized_pnl'].cumsum()
    plt.figure(figsize=(12, 6))
    plt.plot(trades_df['trade_time'], trades_df['cumulative_pnl'], marker='o')
    plt.title('Cumulative Realized PnL Over Time')
    plt.xlabel('Time')
    plt.ylabel('Cumulative PnL')
    plt.grid(True)
    # Link trades to entry orders to get entry times
    trades_df['entry_time'] = trades_df.apply(lambda row: find_entry_time(row, orders_df), axis=1)
    trades_df = trades_df.dropna(subset=['entry_time', 'trade_time'])

    span =  klines_df.head(1)['close_time'] -  klines_df.head(1)['open_time'];

    trades_df = trades_df[trades_df['trade_time'] - trades_df["entry_time"] > span[0]]
    # Remove trades where entry time couldn't be found
    # Convert entry_time to datetime
    trades_df['entry_time'] = pd.to_datetime(trades_df['entry_time'])

    # Sort trades by time and select the specified number of trades
    trades_df.sort_values('trade_time', inplace=True)
    selected_trades = trades_df.sample(num_trades)
    print(f"Selected {len(selected_trades)} trades for visualization.")
    for idx, trade in selected_trades.iterrows():
        # Prepare data for each trade
        trade_time = trade['trade_time']
        entry_price = trade['entry_price']
        entry_time = trade['entry_time']
        exit_price = trade['price']
        side = trade['position_side'].upper()

        # Define time window around the trade
        start_time = entry_time - 15*span[0]
        end_time = trade_time + 15*span[0]
        # Filter klines data for this time window
        klines_subset = klines_df[(klines_df['open_time'] >= start_time) & (klines_df['open_time'] <= end_time)]
        figure = go.Figure(data=[go.Candlestick(
            x=klines_subset['close_time'],
            open=klines_subset['open'],
            high=klines_subset['high'],
            low=klines_subset['low'],
            close=klines_subset['close'],
            showlegend=False,
        )])

        fig = figure.update_layout(xaxis_rangeslider_visible=False)

        fig.add_trace(
            go.Scatter(
                x=[entry_time],
                y=[entry_price],
                mode='markers',
                marker=dict(
                    symbol='triangle-up' if side == 'BID' else 'triangle-down',
                    color='green' if side == 'BID' else 'red',
                    size=12
                ),
                showlegend=False
            ),
        )

        fig.add_trace(
            go.Scatter(
                x=[trade_time],
                y=[exit_price],
                mode='markers',
                marker=dict(
                    symbol='triangle-down' if side == 'BID' else 'triangle-up',
                    color='red' if side == 'BID' else 'green',
                    size=12
                ),
                showlegend=False
            ),
        )

        # Set title for each subplot
        fig.update_xaxes(title_text=f"Trade {idx+1} PNL: {trade['realized_pnl']}")

        fig.show()





    # Define time window around the trade
    start_time = list(trades_df.head(1)['trade_time'])[0]
    end_time = list(trades_df.tail(1)['trade_time'])[-1]
    # Filter klines data for this time window
    klines_subset = klines_df[(klines_df['open_time'] >= start_time) & (klines_df['open_time'] <= end_time)]
    figure = go.Figure(data=[go.Candlestick(
        x=klines_subset['close_time'],
        open=klines_subset['open'],
        high=klines_subset['high'],
        low=klines_subset['low'],
        close=klines_subset['close'],
        showlegend=False,
    )])

    for idx, trade in trades_df.iterrows():
        # Prepare data for each trade
        trade_time = trade['trade_time']
        entry_price = trade['entry_price']
        entry_time = trade['entry_time']
        exit_price = trade['price']
        side = trade['position_side'].upper()




        figure.add_trace(
            go.Scatter(
                x=[entry_time],
                y=[entry_price],
                mode='markers',
                marker=dict(
                    symbol='triangle-up' if side == 'BID' else 'triangle-down',
                    color='green' if side == 'BID' else 'red',
                    size=12
                ),
                showlegend=False
            ),
        )

        figure.add_trace(
            go.Scatter(
                x=[trade_time],
                y=[exit_price],
                mode='markers',
                marker=dict(
                    symbol='triangle-down' if side == 'BID' else 'triangle-up',
                    color='red' if side == 'BID' else 'green',
                    size=12
                ),
                showlegend=False
            ),
        )

    # Set title for each subplot
    figure.update_xaxes(title_text=f"PNL: {sum(trades_df['realized_pnl'])}")

    figure.show()


    # Show the figure
    plt.show()





def estimate_entry_price(row):
    if row['qty'] == 0:
        return None  # Avoid division by zero
    if row['position_side'].upper() == 'ASK':
        # Short position
        entry_price = row['price'] + (row['realized_pnl'] / row['qty'])
    elif row['position_side'].upper() == 'BID':
        # Long position
        entry_price = row['price'] - (row['realized_pnl'] / row['qty'])
    else:
        entry_price = None
    return entry_price
def find_entry_time(trade_row, orders_df):
    symbol = trade_row['symbol']
    exit_time = trade_row['trade_time']
    trade_side = trade_row['position_side'].upper()
    qty = trade_row['qty']

    # Determine the opening side and quantity
    if trade_side == 'ASK':
        opening_side = 'BID'
    elif trade_side == 'BID':
        opening_side = 'ASK'
    else:
        return None

    # Filter orders before the exit time, matching the opening side, symbol, and quantity
    orders_before_exit = orders_df[
        (orders_df['order_time'] <= exit_time) &
        (orders_df['symbol'] == symbol) &
        (orders_df['side'].str.upper() == opening_side) &
        (orders_df['quantity'] == qty)
        ]

    # Find the last order before the exit time
    if not orders_before_exit.empty:
        # Get the most recent order
        entry_order = orders_before_exit.sort_values('order_time').iloc[-1]
        entry_time = entry_order['order_time']
        return entry_time
    else:
        return None  # Could not find an entry order

if __name__ == '__main__':
    main()
