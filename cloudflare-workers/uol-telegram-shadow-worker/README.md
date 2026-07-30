# UOL Telegram Cloudflare Worker

Monitor remoto do Clube UOL que substitui a coleta do Mac e o envio automático
do GitHub Actions. O Worker consulta a listagem a cada minuto, enriquece apenas
ofertas inéditas e envia diretamente ao Telegram.

O Worker do Discord é independente e não compartilha estado, secrets ou
agendamento com este projeto.

## Fluxo

1. um Durable Object Alarm executa a cada minuto;
2. a listagem `https://clube.uol.com.br/?order=new` é obtida por HTTP sem cache;
3. o baseline impede o envio das ofertas existentes durante a implantação;
4. aliases de endereço são reconciliados antes de decidir se o card é inédito;
5. somente cards genuinamente inéditos são abertos e enriquecidos;
6. título, validade, descrição e imagem são extraídos da página de detalhe;
7. a oferta é deduplicada e validada;
8. a mensagem é enviada ao canal principal;
9. campanhas elegíveis de ingressos são encaminhadas ao canal 2;
10. cada resultado e tentativa é persistido separadamente;
11. ofertas recentes ausentes são confirmadas como esgotadas e as mensagens
    correspondentes são editadas.

## Regras

- **Canal principal:** toda oferta nova elegível.
- **Canal 2:** somente campanhas em `/campanhasdeingresso/`, exceto teatro,
  stand-up, partidas, campeonatos, futebol e jogos.
- **Esgotamento:** ausência em pelo menos duas verificações e por pelo menos
  15 minutos, limitada às ofertas decididas nos últimos três dias.
- **Enriquecimento:** apenas ofertas novas; no máximo quatro por rodada.
- **Identidade:** combina slug canônico, variantes históricas e
  `parceiro + código interno da oferta`; alterações de acentuação no endereço
  não criam uma oferta nova.
- **Canonicalização:** corrige percent-encoding, mojibake, acentos perdidos,
  variantes `de`, `João`, `até`, `pós`, `seleção`, `graduação` e correções
  conhecidas do consumidor legado.
- **Reenvio:** conteúdo estrito/solto persiste no estado e a combinação
  título + validade bloqueia reenvio recente por sete dias.
- **Validade:** ofertas expiradas e inícios antigos sem data final são
  descartados conforme a janela de 36 horas.
- **Retenção:** no máximo 300 ofertas recentes e 240 execuções.
- **Retries:** principal, canal 2 e edições de esgotamento possuem contadores,
  erros e confirmações independentes.

## Recursos

- Worker: `uol-telegram-shadow-pilot` (nome histórico preservado para manter o
  Durable Object e o baseline)
- Durable Object: `UolTelegramShadow`
- Agendamento: Durable Object Alarm
- Estado: SQLite interno do Durable Object
- Modo padrão: `DELIVERY_MODE=live`; o modo operacional persistido é controlado
  por `POST /mode`
- Secrets: `ADMIN_TOKEN`, `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`, `CANAL2_ID`

Nenhum valor de secret é armazenado no GitHub ou neste diretório.

## Rotas

- `GET /health`: estado sanitizado, configuração booleana, contagens e últimas
  execuções.
- `POST /run`: coleta manual autenticada.
- `POST /test`: teste autenticado do principal e do canal 2, sem registrar uma
  oferta.
- `POST /mode`: altera imediatamente o modo persistido para `live` ou `shadow`,
  autenticada; corpo JSON `{"mode":"shadow"}`.
- `GET /decisions`: decisões recentes, autenticada.
- `GET /inventory`: inventário observado, autenticada.
- `GET /identity-diagnostics`: aliases ativos, autenticada.
- `POST /repair-identities`: reconciliação idempotente de aliases, autenticada.

As rotas autenticadas exigem `Authorization: Bearer <ADMIN_TOKEN>`.

## Fallback

O workflow `.github/workflows/bot_leouol.yml` permanece disponível apenas por
`workflow_dispatch`. O LaunchAgent do Mac fica descarregado durante a operação
normal. Em rollback:

1. usar `POST /mode` com `shadow` para contenção imediata;
   `DELIVERY_MODE` funciona como padrão quando ainda não existe override
   persistido;
2. executar manualmente o workflow legado ou recarregar o LaunchAgent;
3. investigar o Worker antes de uma nova promoção.

## Comandos

```bash
npm install
npm test
npx wrangler types --include-runtime=false
npm run check
npx wrangler secret list
npm run deploy
```
