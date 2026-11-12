# Sistema de troca de mensagens

Projeto de Sistemas Distribuídos. Bots se conectam num servidor pra fazer login, listar e criar canais. A comunicação é feita com ZeroMQ e tudo roda com Docker Compose.

Pra rodar:

```
docker compose up --build
```

## Linguagens

Fiz sozinho, então usei 2 linguagens:

- Python no servidor e no broker
- JavaScript (Node.js) no bot

## Serialização

Usei MessagePack porque é binário e tem biblioteca pronta pra Python e pra JavaScript, sem precisar de arquivo de schema.

Toda mensagem é um mapa com `tipo` e `timestamp`. A resposta do servidor tem `status` (`ok` ou `erro`) e o campo `erro` com o motivo quando dá errado. Os pedidos são `login` (com `usuario`), `listar_canais` e `criar_canal` (com `canal`).

Dá erro quando o nome do usuário ou do canal não tem de 3 a 20 letras, números ou `_`, e quando o canal já existe. Se o login der erro o bot tenta de novo com outro nome. O bot cria um canal quando existem menos de 5.

## Persistência

Cada servidor tem o seu próprio banco SQLite, num volume separado. Os logins ficam na tabela `logins` (usuário e timestamp) e os canais na tabela `canais`.
