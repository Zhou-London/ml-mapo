"""Configuration for the optimization module.

Centralizes ZMQ endpoints, topics, and solver knobs so ``main.py`` contains
only the pipeline topology and the Node definitions.
"""

from __future__ import annotations

# Upstream endpoints
RISK_ADDR = "tcp://localhost:5556"
FORECAST_ADDR = "tcp://localhost:5557"
TOPIC_COV = b"COV"
TOPIC_ALPHA = b"ALPHA"

# Solver knobs
RISK_AVERSION = 50.0
LONG_ONLY = True

# How long the subscriber blocks on a single poll() call (ms); drives
# shutdown responsiveness when no messages are arriving.
POLL_TIMEOUT_MS = 1000
