# UOL Telegram Shadow Worker

Piloto remoto e independente do Mac para validar a substituição gradual do
coletor e do GitHub Actions no fluxo do Clube UOL.

Este Worker opera exclusivamente em modo sombra:

- não possui token do Telegram;
- não envia nem edita mensagens;
- não escreve no GitHub;
- não compartilha estado com o Worker do Discord;
- registra apenas o que o fluxo equivalente **teria feito**.

## Fluxo

1. um Durable Object Alarm executa a cada minuto;
2. a listagem `https://clube.uol.com.br/?order=new` é obtida por HTTP sem
   cache;
3. a primeira rodada cria um baseline e não simula envio das ofertas já
   existentes;
4. somente cards inéditos são abertos e enriquecidos;
5. título, validade, descrição e imagem são extraídos da página de detalhe;
6. a oferta é deduplicada e validada;
7. o Worker registra se enviaria ao canal principal e se também encaminharia
   ao canal 2;
8. ofertas candidatas que desaparecem da listagem são acompanhadas pelas
   regras atuais de esgotamento.

## Regras reproduzidas

- **Canal principal:** toda oferta nova elegível.
- **Canal 2:** somente campanhas em `/campanhasdeingresso/`, exceto teatro,
  stand-up, partidas, campeonatos, futebol e jogos.
- **Esgotamento:** oferta candidata ausente em pelo menos duas verificações e
  por pelo menos 15 minutos, limitada às ofertas dos últimos três dias.
- **Enriquecimento:** apenas ofertas novas; no máximo quatro por rodada.
- **Validade:** descarta ofertas já expiradas e inícios antigos sem data final,
  seguindo a janela atual de 36 horas.
- **Retenção:** no máximo 300 ofertas recentes e 240 execuções.

## Recursos e isolamento

- Worker: `uol-telegram-shadow-pilot`
- Durable Object: `UolTelegramShadow`
- Agendamento: Durable Object Alarm, sem consumir Cron Trigger
- Estado: SQLite interno do Durable Object
- Secret administrativo: `ADMIN_TOKEN`
- Modo obrigatório: `SHADOW_MODE=1`

Mesmo a rota manual apenas executa uma coleta sombra. Não existe código de
Telegram neste projeto.

## Rotas

- `GET /health`: estado sanitizado, contagens e últimas decisões.
- `POST /run`: coleta manual autenticada.
- `GET /decisions`: decisões recentes, autenticada.
- `GET /inventory`: inventário observado para comparação com o snapshot do
  Mac, autenticada.

As rotas autenticadas exigem `Authorization: Bearer <ADMIN_TOKEN>`.

## Comandos

```bash
npm install
npm test
npx wrangler types
npm run check
npx wrangler secret put ADMIN_TOKEN
npm run deploy
```
