# Data manipulation and analysis
import pandas as pd
import numpy as np

# MongoDB connection
from pymongo import MongoClient
from bson.binary import Binary
from bson.objectid import ObjectId

# UUID decoding
import uuid

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns

# Display settings
plt.style.use('seaborn-darkgrid')
sns.set_context('talk')

# Suppress warnings
import warnings
warnings.filterwarnings('ignore')
# MongoDB connection parameters
mongo_host = 'localhost'
mongo_port = 27017
database_name = 'binance-studies'  # Replace with your database name

# Create a MongoDB client
client = MongoClient(f'mongodb://{mongo_host}:{mongo_port}/')

# Access the database
db = client[database_name]

# Access the collections
past_trades_col = db['past_trades']
orders_col = db['orders']

# Retrieve data from the collections
past_trades_data = list(past_trades_col.find())
orders_data = list(orders_col.find())

print(f"Retrieved {len(past_trades_data)} documents from 'past_trades' collection.")
print(f"Retrieved {len(orders_data)} documents from 'orders' collection.")

# Load data into DataFrames
past_trades_df = pd.DataFrame(past_trades_data)
orders_df = pd.DataFrame(orders_data)

# Function to decode Binary UUIDs
def decode_binary_uuid(val):
    if isinstance(val, Binary):
        return str(uuid.UUID(bytes=val))
    return val

# Function to convert MongoDB Long type to int
def convert_long(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return val

# Decode binary UUIDs
past_trades_df['order_id'] = past_trades_df['order_id'].apply(decode_binary_uuid)

# Convert numeric fields
numeric_fields = ['price', 'commission', 'qty', 'quote_qty', 'realized_pnl']
for field in numeric_fields:
    past_trades_df[field] = pd.to_numeric(past_trades_df[field], errors='coerce')

# Convert time field to datetime
past_trades_df['time'] = pd.to_datetime(past_trades_df['time'], unit='ms')

# Expand the 'symbol' field
symbol_df = past_trades_df['symbol'].apply(pd.Series)
past_trades_df = pd.concat([past_trades_df.drop('symbol', axis=1), symbol_df], axis=1)

# Display the cleaned DataFrame
past_trades_df.head()

# Decode binary UUIDs
orders_df['id'] = orders_df['id'].apply(decode_binary_uuid)

# Convert numeric fields
numeric_fields_orders = ['price', 'quantity', 'lifetime']
for field in numeric_fields_orders:
    orders_df[field] = pd.to_numeric(orders_df[field], errors='coerce')

# Convert time field to datetime
orders_df['time'] = pd.to_datetime(orders_df['time'], unit='ms')

# Expand the 'symbol' field
symbol_orders_df = orders_df['symbol'].apply(pd.Series)
orders_df = pd.concat([orders_df.drop('symbol', axis=1), symbol_orders_df], axis=1)

# Display the cleaned DataFrame
orders_df.head()

# Check for missing values in past_trades_df
# print("Missing values in past_trades_df:")
# print(past_trades_df.isnull().sum())

# Fill or drop missing values as appropriate
# past_trades_df.dropna(inplace=True)

# Check for missing values in orders_df
# print("\nMissing values in orders_df:")
# print(orders_df.isnull().sum())

# Fill or drop missing values as appropriate
# orders_df.dropna(inplace=True)

# Total number of trades
total_trades = past_trades_df.shape[0]
print(f"Total Trades: {total_trades}")

# Total traded volume (quantity)
total_volume = past_trades_df['qty'].sum()
print(f"Total Traded Volume (Qty): {total_volume}")

# Average trade price
average_price = past_trades_df['price'].mean()
print(f"Average Trade Price: {average_price:.2f}")

# Total realized PnL
total_pnl = past_trades_df['realized_pnl'].sum()
print(f"Total Realized PnL: {total_pnl:.2f}")

# Total number of orders
total_orders = orders_df.shape[0]
print(f"\nTotal Orders: {total_orders}")

# Orders by type
orders_by_type = orders_df['order_type'].value_counts()
print("\nOrders by Type:", orders_by_type)

# Average order quantity
average_order_qty = orders_df['quantity'].mean()
print(f"Average Order Quantity: {average_order_qty:.2f}")