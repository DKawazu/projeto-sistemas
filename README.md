# Sistema de troca de mensagens

Projeto de Sistemas Distribuídos. Bots se conectam num servidor pra fazer login, criar canais e publicar mensagens. A comunicação é feita com ZeroMQ e tudo roda com Docker Compose.

Pra rodar:

```
docker compose up --build
```

## Linguagens

Fiz sozinho, então usei 2 linguagens:

- Python no servidor, no broker, no proxy e no serviço de referência
- JavaScript (Node.js) no bot

## Serialização

Usei MessagePack porque é binário e tem biblioteca pronta pra Python e pra JavaScript, sem precisar de arquivo de schema.

Toda mensagem é um mapa com `tipo`, `timestamp` e `relogio` (relógio lógico). A resposta do servidor tem `status` (`ok` ou `erro`) e o campo `erro` com o motivo quando dá errado. Os pedidos são `login` (com `usuario`), `listar_canais`, `criar_canal` (com `canal`) e `publicar` (com `canal` e `texto`).

Dá erro quando o nome do usuário ou do canal não tem de 3 a 20 letras, números ou `_`, quando o canal já existe e quando se publica num canal que não existe. Se o login der erro o bot tenta de novo com outro nome.

## Publicação nos canais

O nome do canal é o tópico do Pub/Sub. Quando o servidor recebe um `publicar`, ele manda pro proxy a mensagem em duas partes (`[canal, mensagem]`) e depois responde o bot. Cada bot usa um só socket SUB e vai se inscrevendo em até 3 canais.

## Persistência

Cada servidor tem o seu próprio banco SQLite, num volume separado. Os logins ficam na tabela `logins` (usuário e timestamp), os canais na tabela `canais` e as publicações na tabela `mensagens` (canal, usuário, texto, timestamp e relógio lógico).
