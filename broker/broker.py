import zmq

ctx = zmq.Context()

clientes = ctx.socket(zmq.ROUTER)
clientes.bind("tcp://*:5555")

servidores = ctx.socket(zmq.DEALER)
servidores.bind("tcp://*:5556")

print("broker rodando (bots: 5555, servidores: 5556)", flush=True)
zmq.proxy(clientes, servidores)
