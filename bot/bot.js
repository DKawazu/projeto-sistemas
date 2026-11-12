const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");

const BROKER = process.env.BROKER || "tcp://broker:5555";

const NOMES = ["ana", "bia", "caio", "duda", "enzo", "gabi", "joao", "lia", "leo", "malu", "nina", "otto", "rafa", "theo"];
const CANAIS = ["geral", "jogos", "musica", "filmes", "estudos", "memes", "futebol", "receitas", "viagens", "livros", "series"];

let usuario = null;
let req = null;

const esperar = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const sortear = (lista) => lista[Math.floor(Math.random() * lista.length)];
const agora = () => Date.now() / 1000;

function hora(ts) {
  if (!ts) return "-";
  return new Date(ts * 1000).toISOString().slice(11, 23);
}

function mostrar(seta, msg) {
  const campos = [];
  for (const [k, v] of Object.entries(msg)) {
    if (k === "tipo" || k === "timestamp") continue;
    if (Array.isArray(v)) campos.push(`${k}=[${v.join(", ")}]`);
    else campos.push(`${k}=${v}`);
  }
  console.log(`[${usuario || "?"}] ${seta} ${msg.tipo} | ts=${hora(msg.timestamp)} | ${campos.join(" ")}`);
}

async function pedir(tipo, campos = {}) {
  while (true) {
    if (!req) {
      req = new zmq.Request({ receiveTimeout: 5000, linger: 0 });
      req.connect(BROKER);
    }

    const msg = { tipo, usuario, timestamp: agora(), ...campos };
    mostrar("->", msg);

    try {
      await req.send(encode(msg));
      const [conteudo] = await req.receive();
      const resp = decode(conteudo);
      mostrar("<-", resp);
      return resp;
    } catch (err) {
      console.log(`[${usuario || "?"}] sem resposta (${err.code || err.message}), tentando de novo`);
      req.close();
      req = null;
      await esperar(1000);
    }
  }
}

async function login() {
  while (true) {
    const nome = `${sortear(NOMES)}_${Math.floor(Math.random() * 100)}`;
    const resp = await pedir("login", { usuario: nome });
    if (resp.status === "ok") {
      usuario = nome;
      console.log(`[${usuario}] login feito`);
      return;
    }
    console.log(`[?] login deu erro: ${resp.erro}. tentando de novo`);
    await esperar(1000);
  }
}

async function listarCanais() {
  const resp = await pedir("listar_canais");
  return resp.status === "ok" ? resp.canais : [];
}

async function criarCanal() {
  const resp = await pedir("criar_canal", { canal: sortear(CANAIS) });
  if (resp.status !== "ok") console.log(`[${usuario}] nao criou o canal: ${resp.erro}`);
}

async function main() {
  await esperar(3000);
  await login();

  while (true) {
    const canais = await listarCanais();
    if (canais.length < 5) {
      await criarCanal();
    }
    await esperar(3000);
  }
}

main();
