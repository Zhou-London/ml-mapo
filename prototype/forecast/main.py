import zmq


def connect() -> zmq.Socket:
    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect("tcp://localhost:5555")
    return sub
