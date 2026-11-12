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

## Replicação

Usei replicação ativa: toda alteração é enviada pra todas as réplicas e cada uma aplica no seu banco. O servidor que recebe o pedido do bot (login, criar canal ou publicar) grava no seu banco e publica a alteração no tópico `replica` do proxy. Os outros servidores estão inscritos nesse tópico e gravam a mesma coisa. Assim todos ficam com todos os dados, não importa pra qual servidor o broker mandou o pedido.

Mudanças que precisei fazer:

- Não usei ordem total nas mensagens de réplica, porque as operações são só inserções e o resultado final é o mesmo em qualquer ordem.
- Cada registro ganhou um id (uuid) e a gravação ignora o que já existe, pra não duplicar.
- O Pub/Sub não guarda mensagem, então um servidor que estava fora perde o que foi publicado nesse tempo. Por isso, quando um servidor sobe ele pede tudo pra outro servidor (pedido `sincronizar` na porta 6000) e grava o que estiver faltando.
