import time

import msgpack
import zmq

TEMPO_LIMITE = 30  # segundos

ranks = {}
vistos = {}
relogio = 0


def fmt(ts):
    return time.strftime("%H:%M:%S", time.gmtime(ts)) + ".%03d" % (int(ts * 1000) % 1000)


def mostrar(seta, msg):
    resto = " ".join(f"{k}={v}" for k, v in msg.items() if k not in ("tipo", "timestamp", "relogio"))
    print(f"[referencia] {seta} {msg.get('tipo')} | ts={fmt(msg.get('timestamp', 0))} relogio={msg.get('relogio')} | {resto}", flush=True)


def resposta(tipo, erro=None, **campos):
    global relogio
    relogio += 1
    msg = {"tipo": tipo, "de": "referencia", "timestamp": time.time(), "relogio": relogio}
    if erro:
        msg["status"] = "erro"
        msg["erro"] = erro
    else:
        msg["status"] = "ok"
        msg.update(campos)
    return msg


def tratar(msg):
    tipo = msg.get("tipo")
    nome = msg.get("nome")

    if tipo == "rank":
        if not nome:
            return resposta(tipo, erro="faltou o nome")
        if nome not in ranks:
            ranks[nome] = len(ranks) + 1
        vistos[nome] = time.time()
        return resposta(tipo, rank=ranks[nome])

    if tipo == "listar":
        lista = [{"nome": n, "rank": ranks[n]} for n in vistos]
        lista.sort(key=lambda s: s["rank"])
        return resposta(tipo, servidores=lista)

    if tipo == "heartbeat":
        if nome not in ranks:
            return resposta(tipo, erro="servidor nao cadastrado, pede o rank de novo")
        if nome not in vistos:
            print(f"[referencia] {nome} voltou pra lista", flush=True)
        vistos[nome] = time.time()
        return resposta(tipo)

    return resposta(tipo, erro="tipo desconhecido")


ctx = zmq.Context()
rep = ctx.socket(zmq.REP)
rep.bind("tcp://*:5559")
print("referencia rodando na porta 5559", flush=True)

poller = zmq.Poller()
poller.register(rep, zmq.POLLIN)

while True:
    if poller.poll(1000):
        msg = msgpack.unpackb(rep.recv())
        relogio = max(relogio, msg.get("relogio", 0))
        mostrar("<-", msg)
        resp = tratar(msg)
        mostrar("->", resp)
        rep.send(msgpack.packb(resp))

    agora = time.time()
    for nome in list(vistos):
        if agora - vistos[nome] > TEMPO_LIMITE:
            del vistos[nome]
            print(f"[referencia] {nome} ficou {TEMPO_LIMITE}s sem heartbeat, tirei da lista", flush=True)
