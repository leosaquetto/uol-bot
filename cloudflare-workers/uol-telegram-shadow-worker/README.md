# UOL Telegram Cloudflare Worker

Monitor remoto do Clube UOL que substitui a coleta do Mac e o envio automático
do GitHub Actions. O Worker consulta a listagem a cada 15 segundos, enriquece apenas
ofertas inéditas e faz o fan-out para Telegram e Discord a partir do mesmo estado.

O Worker antigo do Discord permanece temporariamente implantado em `dry-run`
como rollback, mas não publica mensagens.

## Fluxo

1. um Durable Object Alarm executa a cada 15 segundos;
2. a API de ingressos e a listagem `https://clube.uol.com.br/?order=new` são
   obtidas em paralelo e sem cache;
3. o baseline impede o envio das ofertas existentes durante a implantação;
4. aliases de endereço são reconciliados antes de decidir se o card é inédito;
5. ingressos genuinamente inéditos disparam Telegram e Discord em paralelo com
   a thumbnail, antes de abrir a página de detalhe;
6. título, validade, descrição e imagem são extraídos da página de detalhe;
7. a publicação curta é enriquecida por edição e a oferta é validada;
8. benefícios comuns continuam sendo enviados depois do enriquecimento;
9. campanhas elegíveis de ingressos são encaminhadas ao canal 2;
10. o webhook do Telegram associa a cópia automática no grupo vinculado e
    publica a descrição completa como comentário;
11. cada resultado e tentativa é persistido separadamente;
12. ofertas recentes ausentes são confirmadas como esgotadas e as mensagens
    correspondentes são editadas.

## Regras

- **Canal principal:** toda oferta nova elegível.
- **Canal 2:** somente campanhas em `/campanhasdeingresso/`, exceto teatro,
  stand-up, partidas, campeonatos, futebol e jogos.
- **Esgotamento:** ausência em pelo menos duas verificações e por pelo menos
  15 minutos, limitada às ofertas decididas nos últimos três dias.
- **Enriquecimento:** apenas ofertas novas; no máximo quatro por rodada.
- **Imagem:** reutiliza primeiro o `file_id` devolvido pelo Telegram; depois
  tenta URL pública, proxy já cacheado pelo Discord e upload binário antes de
  recorrer a texto. O cache guarda no máximo 500 imagens e expira usos inativos
  após 90 dias.
- **Circuit breaker de imagem:** cada estratégia abre separadamente após três
  falhas, descansa por dez minutos, faz uma tentativa em `half-open` e fecha
  assim que volta a funcionar.
- **Detalhes estruturados:** validade e endereço são extraídos dos blocos
  próprios da página; abreviações como `Av.` não interrompem o endereço.
- **Discussão:** a publicação principal continua compacta e o texto completo é
  respondido no grupo vinculado `LeoUOL Chat`; chunks confirmados não são
  repetidos em um retry parcial.
- **Discord:** somente campanhas de ingressos, mantendo o embed com thumbnail.
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
- **Retenção:** limpeza automática diária; ofertas terminais e ausentes há mais
  de 30 dias são removidas, mantendo no máximo 300 registros terminais. Cards
  ainda visíveis e entregas pendentes nunca entram nessa limpeza. As últimas
  240 execuções ficam disponíveis por aproximadamente uma hora.
- **Retries:** principal, canal 2 e edições de esgotamento possuem contadores,
  erros e confirmações independentes.
- **Operação:** falha de autorização da API alerta imediatamente; erros comuns
  exigem três ciclos. Também são monitorados três scans quebrados, webhook
  pendente/incorreto e ingresso novo sem foto ou comentário após três minutos.
  Incidentes são deduplicados por chave, têm cooldown de seis horas e ficam no
  SQLite com resolução registrada.
- **Fontes:** mede por oferta se API ou HTML descobriu primeiro e a diferença em
  milissegundos. Alerta somente por listagem vazia/queda repetida, ausência de
  ciclos combinados saudáveis ou divergência total persistente; não alerta
  apenas porque ofertas esgotadas continuam aparecendo na API.
- **Autenticação:** tenta renovar o `X-Authorization` uma hora antes da expiração
  de tokens JWT e imediatamente após 401/403, com cooldown de seis horas. O
  token anterior nunca é apagado numa falha e a listagem HTML continua ativa.
- **Latência:** `/health` mede descoberta até Discord, Telegram, canal 2 e
  comentário para ofertas observadas após a ativação das métricas, com último
  valor, p50, p95 e máximo numa janela de 24 horas.

## Recursos

- Worker: `uol-telegram-shadow-pilot` (nome histórico preservado para manter o
  Durable Object e o baseline)
- Durable Object: `UolTelegramShadow`
- Agendamento: Durable Object Alarm
- Estado: SQLite interno do Durable Object
- Modo padrão: `DELIVERY_MODE=live`; o modo operacional persistido é controlado
  por `POST /mode`
- Secrets: `ADMIN_TOKEN`, `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`, `CANAL2_ID`,
  `TELEGRAM_WEBHOOK_SECRET`, `DISCORD_WEBHOOK_URL` e, opcionalmente,
  `OPS_TELEGRAM_CHAT_ID`. A renovação por navegador usa ainda
  `UOL_LOGIN_USERNAME` e `UOL_LOGIN_PASSWORD`. Sem o chat operacional, alertas
  usam o canal principal.

Nenhum valor de secret é armazenado no GitHub ou neste diretório.

## Rotas

- `GET /health`: estado sanitizado, configuração booleana, contagens, últimas
  execuções, latências e incidentes operacionais.
- `GET /dashboard`: painel HTML operacional; aceita Bearer ou HTTP Basic com
  usuário `admin` e senha igual ao `ADMIN_TOKEN`.
- `GET /dashboard.json`: os mesmos dados estruturados e autenticados.
- `POST /refresh-auth`: força uma tentativa de renovação da autorização pessoal.
- `POST /run`: coleta manual autenticada.
- `POST /test`: teste autenticado do principal e do canal 2, sem registrar uma
  oferta.
- `POST /mode`: altera imediatamente o modo persistido para `live` ou `shadow`,
  autenticada; corpo JSON `{"mode":"shadow"}`.
- `GET /decisions`: decisões recentes, autenticada.
- `GET /inventory`: inventário observado, autenticada.
- `GET /identity-diagnostics`: aliases ativos, autenticada.
- `POST /repair-identities`: reconciliação idempotente de aliases, autenticada.
- `POST /telegram-webhook`: entrada validada por secret para encaminhamentos
  automáticos do canal ao grupo de discussão.

As rotas autenticadas exigem `Authorization: Bearer <ADMIN_TOKEN>`.

## Fallback arquivado

Os workflows UOL estão desativados no GitHub e preservados fora da pasta de
execução em `.github/workflows-archive/`. O LaunchAgent foi removido do domínio
do usuário e preservado em `~/Library/LaunchAgents.disabled/`. Em rollback:

1. usar `POST /mode` com `shadow` para contenção imediata;
   `DELIVERY_MODE` funciona como padrão quando ainda não existe override
   persistido;
2. remover o webhook do Telegram antes de reativar um consumidor que use
   `getUpdates`;
3. restaurar conscientemente um workflow arquivado ou o LaunchAgent desativado;
4. investigar o Worker antes de uma nova promoção.

## Orçamento no tier gratuito

Em estado estável, o alarme de 15 segundos representa 5.760 invocações por dia,
172.800 em 30 dias. Cada ciclo saudável faz duas leituras externas em paralelo
(API e HTML). Browser Rendering não é usado em cada scan: só quando uma
renovação de autenticação vence o controle de expiração/erro e cooldown.
As ofertas conhecidas só são atualizadas se algum campo realmente mudar. Sem
oferta nova, o SQLite escreve aproximadamente 17.280 linhas por dia: alarme,
registro da execução e descarte do registro mais antigo. Enriquecimento,
Telegram, comentários e Discord só acrescentam operações quando existe novidade.
A limpeza das ofertas roda apenas uma vez por dia e acrescenta escritas somente
quando efetivamente encontra registros antigos.

## Comandos

```bash
npm install
npm test
npx wrangler types --include-runtime=false
npm run check
npx wrangler secret list
npm run deploy
```
