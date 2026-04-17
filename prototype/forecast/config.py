"""Configuration for the forecast module.

Centralizes ZMQ endpoints and topics so ``main.py`` contains only the pipeline
topology and the Node definitions.
"""

from __future__ import annotations

DATA_ADDR = "tcp://localhost:5555"
PUB_ADDR = "tcp://*:5557"
TOPIC_OHLCV = b"OHLCV"
TOPIC_ALPHA = b"ALPHA"
