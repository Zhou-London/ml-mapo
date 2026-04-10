from time import sleep

import zmq
import yfinance

target_tickers = ["AAPL", "GOOG", "AMZN", "MSFT", "TSLA", "META", "NVDA"]

# Create socket
def connect_socket() -> zmq.Socket:
    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    pub.bind("tcp://*:5555")
    return pub

# Connect to TimescaleDB
def connect_database():
    # placeholder

def main():
    # Get tickers's past 12 month data and store into DB