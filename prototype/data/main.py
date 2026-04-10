from time import sleep

import zmq

# Create socket
def connect() -> zmq.Socket:
    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    pub.bind("tcp://*:5555")
    return pub

def main():
