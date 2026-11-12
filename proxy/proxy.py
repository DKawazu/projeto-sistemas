import zmq

ctx = zmq.Context()

entrada = ctx.socket(zmq.XSUB)
entrada.bind("tcp://*:5557")

saida = ctx.socket(zmq.XPUB)
saida.bind("tcp://*:5558")

print("proxy rodando (XSUB: 5557, XPUB: 5558)", flush=True)
zmq.proxy(entrada, saida)
