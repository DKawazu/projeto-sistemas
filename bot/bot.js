const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");

const BROKER = process.env.BROKER || "tcp://broker:5555";
const PROXY = process.env.PROXY || "tcp://proxy:5558";

const NOMES = ["ana", "bia", "caio", "duda", "enzo", "gabi", "joao", "lia", "leo", "malu", "nina", "otto", "rafa", "theo"];
const CANAIS = ["geral", "jogos", "musica", "filmes", "estudos", "memes", "futebol", "receitas", "viagens", "livros", "series"];
const FRASES = [
  "oi",
  "alo",
  "teste",
  "testando",
  "testando 1 2 3",
  "mensagem de teste",
  "oi pessoal",
  "alguem ai?",
  "bom dia",
  "tudo bem?",
  "ok",
];

let usuario = null;
let relogio = 0;
let req = null;
const sub = new zmq.Subscriber();
const inscritos = new Set();

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
    if (k === "tipo" || k === "timestamp" || k === "relogio") continue;
    if (Array.isArray(v)) campos.push(`${k}=[${v.join(", ")}]`);
    else campos.push(`${k}=${v}`);
  }
  console.log(`[${usuario || "?"}] ${seta} ${msg.tipo} | ts=${hora(msg.timestamp)} relogio=${msg.relogio} | ${campos.join(" ")}`);
}

async function pedir(tipo, campos = {}) {
  while (true) {
    if (!req) {
      req = new zmq.Request({ receiveTimeout: 5000, linger: 0 });
      req.connect(BROKER);
    }

    relogio++;
    const msg = { tipo, usuario, timestamp: agora(), relogio, ...campos };
    mostrar("->", msg);

    try {
      await req.send(encode(msg));
      const [conteudo] = await req.receive();
      const resp = decode(conteudo);
      relogio = Math.max(relogio, resp.relogio || 0);
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

function inscrever(canais) {
  const livres = canais.filter((c) => !inscritos.has(c));
  if (!livres.length) return;
  const canal = sortear(livres);
  sub.subscribe(canal);
  inscritos.add(canal);
  console.log(`[${usuario}] me inscrevi em #${canal} (inscrito em: ${[...inscritos].join(", ")})`);
}

async function ouvir() {
  for await (const [topico, conteudo] of sub) {
    const canal = topico.toString();
    // o filtro do zmq e por prefixo
    if (!inscritos.has(canal)) continue;
    const msg = decode(conteudo);
    relogio = Math.max(relogio, msg.relogio || 0);
    console.log(
      `[${usuario}] <- #${canal} ${msg.usuario}: "${msg.texto}" | enviado=${hora(msg.timestamp)} ` +
        `recebido=${hora(agora())} relogio=${msg.relogio} (publicado por ${msg.de})`
    );
  }
}

async function main() {
  await esperar(3000);
  sub.connect(PROXY);
  await login();
  ouvir();

  while (true) {
    let canais = await listarCanais();
    if (canais.length < 5) {
      await criarCanal();
      canais = await listarCanais();
    }
    if (inscritos.size < 3) {
      inscrever(canais);
    }
    if (!canais.length) {
      await esperar(1000);
      continue;
    }

    const canal = sortear(canais);
    console.log(`[${usuario}] vou mandar 10 mensagens em #${canal}`);
    for (let i = 0; i < 10; i++) {
      const resp = await pedir("publicar", { canal, texto: sortear(FRASES) });
      if (resp.status !== "ok") console.log(`[${usuario}] erro ao publicar: ${resp.erro}`);
      await esperar(1000);
    }
  }
}

main();
