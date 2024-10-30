import sqlite3
import pandas as pd

# Replace 'your_database.db' with the path to your SQLite database file
conn = sqlite3.connect('binance_studies.db')
trades_df = pd.read_sql_query("SELECT * FROM trades", conn)

# Load orders data
orders_df = pd.read_sql_query("SELECT * FROM orders", conn)

# Load klines data
klines_df = pd.read_sql_query("SELECT * FROM klines", conn)
# Ensure correct data types
trades_df['realized_pnl'] = trades_df['realized_pnl'].astype(float)
trades_df['price'] = trades_df['price'].astype(float)
trades_df['qty'] = trades_df['qty'].astype(float)
trades_df['side'] = trades_df['side'].astype(str)

# Define a function to calculate entry price
def estimate_entry_price(row):
    if row['qty'] == 0:
        return None  # Avoid division by zero
    if row['position_side'].upper() == 'ASK':
        # Short position
        entry_price = row['price'] - (row['realized_pnl'] / row['qty'])
    elif row['position_side'].upper() == 'BID':
        # Long position
        entry_price = row['price'] + (row['realized_pnl'] / row['qty'])
    else:
        entry_price = None
    return entry_price

# Apply the function to estimate entry prices
trades_df['entry_price'] = trades_df.apply(estimate_entry_price, axis=1)

# Convert timestamps to datetime
klines_df['open_time'] = pd.to_datetime(klines_df['open_time'], unit='ms')
klines_df['close_time'] = pd.to_datetime(klines_df['close_time'], unit='ms')

# Sort by time
klines_df.sort_values('open_time', inplace=True)
# Convert 'time' in trades to datetime
trades_df['trade_time'] = pd.to_datetime(trades_df['time'], unit='ms')

# Separate trades based on side
long_trades = trades_df[trades_df['position_side'].str.upper() == 'ASK']
short_trades = trades_df[trades_df['position_side'].str.upper() == 'BID']
import plotly.graph_objects as go
# Create candlestick chart
fig = go.Figure(data=[go.Line(
    x=klines_df['open_time'],
    y=klines_df['open'],
    # high=klines_df['high'],
    # low=klines_df['low'],
    # close=klines_df['close'],
)])

# # Add entry and exit points for long trades
fig.add_trace(go.Scatter(
    x=long_trades['trade_time'],
    y=long_trades['entry_price'],
    mode='markers',
    marker=dict(symbol='triangle-up', color='blue', size=10),
    name='Long Entry'
))


# print(long_trades)
#
fig.add_trace(go.Scatter(
    x=long_trades['trade_time'],
    y=long_trades['price'],
    mode='markers',
    marker=dict(symbol='triangle-down', color='red', size=10),
    name='Long Exit'
))

# # Add entry and exit points for short trades
fig.add_trace(go.Scatter(
    x=short_trades['trade_time'],
    y=short_trades['entry_price'],
    mode='markers',
    marker=dict(symbol='triangle-down', color='orange', size=10),
    name='Short Entry'
))
#
fig.add_trace(go.Scatter(
    x=short_trades['trade_time'],
    y=short_trades['price'],
    mode='markers',
    marker=dict(symbol='triangle-up', color='green', size=10),
    name='Short Exit'
))

# Update layout
fig.update_layout(
    title='Backtest Trades on Market Data',
    xaxis_title='Time',
    yaxis_title='Price',
    xaxis_rangeslider_visible=False,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
)

# Show the figure
fig.show()

# Calculate cumulative PnL

# Plot cumulative PnL over time
import matplotlib.pyplot as plt

plt.figure(figsize=(12, 6))
plt.plot(trades_df['trade_time'], trades_df['cumulative_pnl'], marker='o')
plt.title('Cumulative Realized PnL Over Time')
plt.xlabel('Time')
plt.ylabel('Cumulative PnL')
plt.grid(True)
plt.show()
