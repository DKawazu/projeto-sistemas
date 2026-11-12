import os
import re
import sqlite3
import time

import msgpack
import zmq

NOME = os.environ.get("NOME", "servidor")
BROKER = os.environ.get("BROKER", "tcp://broker:5556")
PROXY_XSUB = os.environ.get("PROXY_XSUB", "tcp://proxy:5557")
REFERENCIA = os.environ.get("REFERENCIA", "tcp://referencia:5559")
BANCO = os.environ.get("BANCO", "/dados/dados.db")

NOME_VALIDO = re.compile(r"^[a-zA-Z0-9_]{3,20}$")

ajuste = 0.0
relogio = 0
meu_rank = None

ctx = zmq.Context()
pub = None


def hora():
    return time.time() + ajuste


def tick():
    global relogio
    relogio += 1
    return relogio


def atualiza_relogio(recebido):
    global relogio
    relogio = max(relogio, recebido or 0)


def nova_msg(tipo, **campos):
    msg = {"tipo": tipo, "de": NOME, "timestamp": hora(), "relogio": tick()}
    msg.update(campos)
    return msg


def resposta(tipo, erro=None, **campos):
    if erro:
        return nova_msg(tipo, status="erro", erro=erro)
    return nova_msg(tipo, status="ok", **campos)


def fmt(ts):
    if not ts:
        return "-"
    return time.strftime("%H:%M:%S", time.gmtime(ts)) + ".%03d" % (int(ts * 1000) % 1000)


def mostrar(seta, msg, onde=""):
    campos = []
    for k, v in msg.items():
        if k in ("tipo", "timestamp", "relogio"):
            continue
        campos.append(f"{k}={v}")
    print(f"[{NOME}] {seta} {onde}{msg.get('tipo')} | ts={fmt(msg.get('timestamp'))} "
          f"relogio={msg.get('relogio')} | {' '.join(campos)}", flush=True)


def pedir(endereco, msg, timeout=2000):
    s = ctx.socket(zmq.REQ)
    s.setsockopt(zmq.LINGER, 0)
    s.setsockopt(zmq.RCVTIMEO, timeout)
    s.connect(endereco)
    try:
        mostrar("->", msg, f"({endereco}) ")
        s.send(msgpack.packb(msg))
        resp = msgpack.unpackb(s.recv())
        atualiza_relogio(resp.get("relogio"))
        mostrar("<-", resp)
        return resp
    except zmq.Again:
        print(f"[{NOME}] {endereco} nao respondeu", flush=True)
        return None
    finally:
        s.close()


def publicar(topico, msg):
    mostrar("->", msg, f"PUB[{topico}] ")
    pub.send_multipart([topico.encode(), msgpack.packb(msg)])


def abrir_banco():
    os.makedirs(os.path.dirname(BANCO), exist_ok=True)
    con = sqlite3.connect(BANCO)
    con.execute("create table if not exists logins (id integer primary key autoincrement, usuario text, timestamp real)")
    con.execute("create table if not exists canais (nome text primary key, criado_por text, timestamp real)")
    con.execute("create table if not exists mensagens (id integer primary key autoincrement, canal text, usuario text, "
                "texto text, timestamp real, relogio integer)")
    con.commit()
    return con


banco = abrir_banco()


def tratar_login(msg):
    usuario = msg.get("usuario") or ""
    if not NOME_VALIDO.match(usuario):
        return resposta("login", erro="nome invalido, use de 3 a 20 letras, numeros ou _")
    banco.execute("insert into logins (usuario, timestamp) values (?, ?)", (usuario, hora()))
    banco.commit()
    return resposta("login", usuario=usuario)


def tratar_criar_canal(msg):
    canal = msg.get("canal") or ""
    if not NOME_VALIDO.match(canal):
        return resposta("criar_canal", erro=f"nome de canal invalido: {canal}")
    if banco.execute("select 1 from canais where nome = ?", (canal,)).fetchone():
        return resposta("criar_canal", erro=f"o canal {canal} ja existe")
    banco.execute("insert into canais (nome, criado_por, timestamp) values (?, ?, ?)",
                  (canal, msg.get("usuario"), hora()))
    banco.commit()
    return resposta("criar_canal", canal=canal)


def tratar_listar_canais(msg):
    canais = [linha[0] for linha in banco.execute("select nome from canais order by nome")]
    return resposta("listar_canais", canais=canais)


def tratar_publicar(msg):
    canal = msg.get("canal")
    texto = msg.get("texto") or ""
    if not banco.execute("select 1 from canais where nome = ?", (canal,)).fetchone():
        return resposta("publicar", erro=f"o canal {canal} nao existe")
    if not texto.strip():
        return resposta("publicar", erro="mensagem vazia")

    publicacao = nova_msg("publicacao", canal=canal, usuario=msg.get("usuario"), texto=texto)
    publicar(canal, publicacao)

    banco.execute("insert into mensagens (canal, usuario, texto, timestamp, relogio) values (?, ?, ?, ?, ?)",
                  (canal, msg.get("usuario"), texto, publicacao["timestamp"], publicacao["relogio"]))
    banco.commit()
    return resposta("publicar", canal=canal)


TRATADORES = {
    "login": tratar_login,
    "criar_canal": tratar_criar_canal,
    "listar_canais": tratar_listar_canais,
    "publicar": tratar_publicar,
}


def registrar():
    global meu_rank
    while True:
        resp = pedir(REFERENCIA, nova_msg("rank", nome=NOME))
        if resp and resp.get("status") == "ok":
            meu_rank = resp["rank"]
            print(f"[{NOME}] meu rank e {meu_rank}", flush=True)
            return
        time.sleep(2)


def heartbeat():
    global ajuste
    antes = time.time()
    resp = pedir(REFERENCIA, nova_msg("heartbeat", nome=NOME))
    depois = time.time()
    if not resp or resp.get("status") != "ok":
        return

    certa = resp["hora"] + (depois - antes) / 2
    correcao = certa - (depois + ajuste)
    ajuste += correcao
    print(f"[{NOME}] relogio corrigido em {correcao:+.3f}s (ajuste total {ajuste:+.3f}s)", flush=True)


def main():
    global pub
    print(f"[{NOME}] subindo", flush=True)

    pub = ctx.socket(zmq.PUB)
    pub.connect(PROXY_XSUB)

    registrar()

    rep = ctx.socket(zmq.REP)
    rep.connect(BROKER)
    print(f"[{NOME}] pronto, esperando os bots", flush=True)

    recebidas = 0
    while True:
        msg = msgpack.unpackb(rep.recv())
        atualiza_relogio(msg.get("relogio"))
        mostrar("<-", msg)

        tratador = TRATADORES.get(msg.get("tipo"))
        if tratador:
            resp = tratador(msg)
        else:
            resp = resposta(msg.get("tipo"), erro="tipo desconhecido")

        mostrar("->", resp)
        rep.send(msgpack.packb(resp))

        recebidas += 1
        if recebidas % 10 == 0:
            heartbeat()


if __name__ == "__main__":
    main()
