# UOL Telegram Cloudflare Worker

Monitor remoto do Clube UOL que substitui a coleta do Mac e o envio automático
do GitHub Actions. O Worker consulta a API completa a cada 15 segundos, persiste
a decisão antes de qualquer chamada externa. Polling principal e manutenção
usam alarmes independentes, mas o mesmo outbox canônico.

Este documento descreve o contrato do código neste checkout. Ele só passa a
descrever produção depois de publicação deliberada e `postdeploy:check`; teste
local verde, commit ou push isoladamente não são recibo de ativação.

A operação é totalmente headless: não depende de dashboard nem de ação manual
para descobrir, enviar ou editar ofertas. As rotas autenticadas de diagnóstico
e o snapshot do Worker são opcionais, usados apenas para prova e recuperação.

O Worker antigo do Discord permanece no repositório como rollback. O coletor
falha fechado: ausente ou diferente de `COLLECTOR_ENABLED=true`, não publica nem
agenda coleta. Estado de produção exige verificação própria.

## Fluxo

1. um Durable Object Alarm inicia o ciclo a cada 15 segundos;
2. a API completa é consultada sozinha e sem cache; HTML e manutenção do webhook
   não competem com essa chamada;
3. o baseline impede o envio das ofertas existentes durante a implantação;
4. aliases, validade e deduplicação são decididos e persistidos no SQLite antes
   de qualquer envio;
5. novidades da API entram imediatamente no outbox durável; o canal principal
   tenta foto da própria API e aguarda no máximo 60 segundos, sem depender de
   preview ou confirmação do HTML;
6. em rajadas, o alarme crítico despacha os canais principais com concorrência
   limitada; novidade antecipa o outro Durable Object, que envia Discord,
   publica URL pura, validade, parceiro, categoria e resumo quando disponíveis,
   recupera a thumbnail e só então encaminha ao canal 2, sem segurar a próxima
   consulta da API;
7. as duas páginas HTML são reconciliadas a cada 60 segundos, ou imediatamente
   se a API falhar/voltar vazia; elas são fallback de descoberta e autoridade de
   esgotamento/reabertura, nunca confirmação prévia para publicar;
8. se nenhuma foto funcionar em 60 segundos, o canal principal recebe texto sem
   preview; uma foto obtida depois converte a mesma mensagem em foto + legenda;
9. campanhas elegíveis de ingressos são copiadas com `copyMessage` ao canal 2;
10. o webhook do Telegram associa a cópia automática no grupo vinculado,
    reconcilia automaticamente um envio principal de resultado incerto e
    publica a descrição completa como comentário;
11. cada destino possui tentativa, backoff, in-flight, resultado incerto e
    confirmação independentes;
12. ofertas recentes ausentes no HTML saudável são confirmadas como esgotadas;
    se reaparecerem no HTML, as mensagens voltam ao estado disponível sem perder
    entregas secundárias que já estavam pendentes; mensagem apagada encerra a
    edição de esgotamento e gera nova publicação somente quando a oferta volta.
13. um watchdog externo consulta apenas `livez`/`readyz`, enquanto o próprio
    Worker mantém ledger de eventos, SLO de fila, backpressure de cota e canário
    do contrato da API; tudo isso permanece invisível nos canais de ofertas.

## Regras

- **Canal principal:** toda oferta nova elegível.
- **Canal 2:** toda campanha em `/campanhasdeingresso/`, incluindo teatro,
  stand-up e esporte.
- **Esgotamento:** ingressos usam duas sondas rápidas da URL da própria oferta,
  com cinco segundos entre confirmações; as demais ofertas continuam exigindo
  ausência em pelo menos duas verificações HTML e 15 minutos. Tudo é limitado às
  ofertas decididas nos últimos três dias. A publicação principal, as cópias do
  canal exclusivo e o Discord são editados idempotentemente. Falha de edição
  entra em backoff; não cria aviso substituto sujeito a duplicata.
- **Enriquecimento:** a API já entrega título, validade, descrição, imagem e
  link. Nada abre HTML de detalhe ou Browser Rendering antes/depois do envio.
- **Imagem:** o canal principal tenta `file_id`, URL e upload até o prazo absoluto
  de 60 segundos. Novas mutações param 38 segundos antes para reservar timeout
  e reconciliação de resultado incerto. Depois envia texto sem preview. Foto tardia usa
  `editMessageMedia` e conserva o mesmo `message_id`, com foto + legenda.
  Para ingressos, o proxy já criado pelo Discord é reutilizado antes do upload.
  `DISCORD_IMAGE_CACHE_WEBHOOK_URL` publica as ofertas comuns em um segundo canal
  Discord como `Clube UOL` e fornece o mesmo proxy de foto. Para ingressos, o
  principal e o Canal 2 são concluídos no mesmo ciclo rápido: o segundo usa
  `copyMessage` logo após a confirmação do primeiro, sem aguardar manutenção.
- **Discussão:** a publicação principal continua compacta e o texto completo é
  respondido no grupo vinculado `LeoUOL Chat`; chunks confirmados não são
  repetidos em um retry parcial.
- **Alertas operacionais:** são enviados diretamente ao `LeoUOL Chat`, sem
  poluir o canal principal de ofertas.
- **Discord:** ingressos no canal original e ofertas comuns no segundo webhook,
  ambos com embed, thumbnail e edição automática de esgotamento/reabertura.
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
  ainda visíveis e entregas pendentes nunca entram nessa limpeza. Eventos,
  falhas, recuperações e amostras saudáveis a cada 15 minutos formam um histórico
  de até 240 execuções.
- **Retries/outbox:** principal, canal 2, Discord e comentários possuem estado
  independente, backoff exponencial com jitter, respeito a `retry_after`,
  in-flight persistido, dead letter e reprocessamento administrativo. Timeout ou
  resposta aceita sem recibo fica `unknown`. No principal, o forward automático
  tem 30 segundos para confirmar o envio; sem confirmação, o Worker tenta de
  novo. A prioridade é não perder oferta, aceitando uma duplicata rara. Os
  destinos secundários continuam exigindo reconciliação ou resolução explícita.
- **Ledger e proteção:** cada tentativa, sucesso, falha, resultado incerto,
  edição, recuperação e mensagem ausente entra no sidecar `delivery_events`, sem
  adicionar colunas à tabela de ofertas. O ledger é idempotente e limitado por
  oferta; cada manutenção reconcilia no máximo 32 eventos recentes usando o
  estado autoritativo, sem repetir um resultado ambíguo. `/decisions` e
  `/inventory` expõem apenas a linha do tempo sanitizada quando autenticados.
- **SLO e cota:** a fila mede idade e p95; novas entregas e o principal mantêm
  prioridade, enquanto comentários, imagens e manutenção secundária cedem antes
  da reserva de leituras do tier gratuito. Um alerta só abre após três violações
  consecutivas, sem bloquear o caminho crítico.
- **Operação:** falha de autorização da API primária alerta imediatamente;
  erros comuns exigem três ciclos, e sua credencial técnica gera aviso 14 dias
  antes de expirar. Também são monitorados três scans quebrados, webhook
  pendente/incorreto e ingresso novo sem comentário após três minutos. Texto
  marcado como `text_timeout` entra na fila de atualização tardia de imagem.
  Incidentes são deduplicados por chave, têm cooldown de seis horas e ficam no
  SQLite com resolução registrada.
- **Fontes:** mede por oferta se API ou HTML descobriu primeiro e a diferença em
  milissegundos. A página pública exclusiva de ingressos é independente da
  listagem geral. Alerta somente por listagem vazia/queda repetida, ausência de
  ciclos combinados saudáveis ou divergência total persistente; não alerta
  apenas porque ofertas esgotadas continuam aparecendo na API.
- **Autenticação:** uma sonda histórica confirmou que a API aceita somente o
  `Authorization` técnico e rejeita requisições sem ele; `X-Authorization`,
  login UOL, senha e token pessoal não são necessários. A API é a fonte primária
  de publicação; se a credencial técnica falhar, as duas páginas HTML públicas
  entram no mesmo ciclo como fallback degradado e o incidente é alertado.
- **Latência:** o diagnóstico autenticado mede descoberta até Discord, Telegram,
  canal 2 e comentário para ofertas observadas após a ativação das métricas, com
  último valor, p50, p95 e máximo numa janela de 24 horas.

## Recursos

- Worker: `uol-telegram-shadow-pilot` (nome histórico preservado para manter o
  Durable Object e o baseline)
- Durable Objects: `UolTelegramShadow` (SQLite/outbox/polling) e
  `UolTelegramMaintenance` (relógio independente)
- Agendamento: polling em 15 segundos; manutenção em 60 segundos, antecipável
  quando a API falha
- Estado: SQLite interno do Durable Object
- Modo padrão: `DELIVERY_MODE=live`; o modo operacional persistido é controlado
  por `POST /mode`
- Versão: o binding `WORKER_VERSION` expõe ID, tag e timestamp da versão
  publicada no diagnóstico operacional.
- Secrets obrigatórios: `ADMIN_TOKEN`, `UOL_API_AUTHORIZATION`, `TELEGRAM_TOKEN`,
  `TELEGRAM_CHAT_ID`, `CANAL2_ID`, `GRUPO_COMENTARIO_ID`,
  `OPS_TELEGRAM_CHAT_ID`, `TELEGRAM_WEBHOOK_SECRET` e `DISCORD_WEBHOOK_URL`.
  `DISCORD_OPS_WEBHOOK_URL` é opcional e cria um segundo transporte para alertas.
  `DISCORD_IMAGE_CACHE_WEBHOOK_URL` é opcional e aponta para o segundo canal que
  recebe as ofertas comuns e também fornece suas thumbnails ao Telegram.
  Não há automação ativa de senha ou login pessoal.

Nenhum valor de secret é armazenado no GitHub ou neste diretório.

## Rotas

- `GET /livez`: liveness público, cacheável e sem consultar o Durable Object.
- `GET /health` e `GET /readyz`: readiness público mínimo; retornam `200` quando
  modo, scan, alarme, configuração, fila e incidentes estão saudáveis, ou `503`
  com os checks sanitizados quando o Worker não está pronto.
- `GET /dashboard`: painel HTML operacional legado; aceita Bearer ou HTTP Basic
  com usuário `admin` e senha igual ao `ADMIN_TOKEN`. É diagnóstico opcional; o
  envio headless não depende dele.
- `GET /dashboard.json`: diagnóstico completo e autenticado, com versão,
  configuração booleana, contagens, execuções, latências e incidentes.
- `GET /offers`: últimas ofertas principais enviadas; JSON público sanitizado,
  cache de 30 segundos, ETag e limite máximo de 12.
- `POST /run`: polling manual autenticado; também garante os dois alarmes.
- `POST /maintenance`: manutenção manual autenticada, incluindo reconciliação HTML.
- `GET /maintenance-status`: próximo alarme de manutenção, autenticada.
- `POST /mode`: altera imediatamente o modo persistido para `live` ou `shadow`,
  autenticada; corpo JSON `{"mode":"shadow"}`.
- `POST /requeue-delivery`: reprocessa `offer`, `main`, `canal2`, `discord` ou
  `comment` em dead letter/quarentena. Requeue manual de `unknown` exige
  `/resolve-delivery`; o principal já possui retry automático após 30 segundos.
- `POST /resolve-delivery`: registra a confirmação humana de um resultado
  `unknown` como `sent` (com o ID externo) ou `not_sent` (retry seguro), sem
  duplicar silenciosamente. O resultado `closed` existe somente para encerrar
  um `unknown` histórico de comentário sem reenviar a oferta.
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

O polling mantém 5.760 consultas da API por dia. A manutenção roda 1.440 vezes e
faz duas leituras HTML por reconciliação. Browser Rendering não faz parte do
Worker.

Telemetria de API, HTML, fontes, webhook e manutenção usa snapshots JSON, em vez
de uma linha por campo. Observações e cards conhecidos só tocam `last_seen_at` a
cada 15 minutos, salvo mudança real. Ciclos `no_change` entram no histórico
`runs` somente como amostra a cada 15 minutos; eventos, falhas e recuperações
continuam imediatos. Cada handler periódico rearma seu alarme uma vez.

As leituras SQLite reais são medidas por `rowsRead` e acumuladas por dia UTC.
A manutenção para antes de invadir a reserva do polling principal. Se o custo
observado crescer, o polling desacelera automaticamente; sem orçamento seguro,
ele rearma para depois do reset diário. Os dois alarmes se rearmam antes de
qualquer leitura pesada, então uma falha de cota não interrompe a retomada.

O diagnóstico calcula o orçamento em `usageEstimate.durableObjectRowsWrittenPerDay`.
Com 48 cards ativos na API e 48 no HTML, a projeção conservadora é 57.920
linhas/dia, já incluindo reserva de 20.000 para entregas e incidentes: margem de
42.080 contra o limite gratuito de 100.000. O valor real varia com cards ativos,
novidades e falhas; `withinFreeTier=false` exige redução de carga antes de deploy.

## Desenvolvimento e CI

O projeto usa Node.js 22.23.2. O caminho local padrão permanece curto para não
sobrecarregar o Mac:

- `npm test` ou `npm run check:fast`: testes puros em Node, sem `workerd`;
  a concorrência fica limitada a dois arquivos para não saturar Macs mais lentos;
- `node --test test/replay.test.js`: replay determinístico de entrega, imagem
  tardia, esgotamento/reabertura, timeout ambíguo, mensagem apagada e rajada de
  24 ofertas, sem tocar Telegram, Discord ou dados reais;
- `npm run test:worker`: integração real com rotas, SQLite Durable Object e
  alarmes, sem rede externa, produção ou secrets. O Workerd atual exige macOS
  13.5 ou superior; em versões anteriores, essa etapa roda no CI Linux;
- `npm run check:ci`: suíte completa, tipos, startup e bundle dry-run. Esse é o
  comando executado no Ubuntu pelo workflow `UOL Worker CI`;
- `npm run deploy`: executa automaticamente o `predeploy` leve (`check:fast` e
  tipos) e, somente depois, cria o bundle e publica conscientemente, sem fazer
  dois dry-runs consecutivos no Mac;
- `npm run postdeploy:check`: cruza `/livez` e `/readyz`, verificando versão,
  scan recente, alarme, modo, configuração, fila e incidentes críticos. Para
  exigir versão, use `EXPECTED_VERSION_ID` do deploy recém-publicado. Para modo,
  use também `EXPECTED_DELIVERY_MODE=live` ou `shadow`.

O workflow de CI não possui permissão de escrita, não lê secrets e nunca faz
deploy. Os testes Cloudflare usam `wrangler.test.jsonc`, isolado da configuração
de produção e com valores explicitamente fictícios. Um job Python 3.11 separado
instala `requirements.txt`, testa e compila o fallback legado. Outro job executa
testes e bundle dry-run do Worker Discord de rollback. Nenhum deles faz parte do
`check:fast` local do Worker principal.

O workflow independente `UOL Worker Ready Monitor` consulta liveness e readiness
públicos a cada cinco minutos. Uma mudança para outage abre um único issue de
incidente; degradação histórica fica registrada sem transformar o job em falha
recorrente, e a primeira verificação saudável fecha o issue.
Esse dead-man externo não depende do Mac, do Telegram ou de secrets e recebe
somente a permissão de issues necessária para registrar e encerrar o incidente.
O agendamento fica inerte até a variável de repositório
`UOL_READY_MONITOR_ENABLED=true`; ela deve ser habilitada somente depois do
primeiro deploy que disponibilizar `/readyz`. `workflow_dispatch` continua
permitido antes disso para uma prova manual controlada.

## Comandos

```bash
npm ci
npm test
npm run test:worker
npm run check:ci
npx wrangler secret list
npm run deploy
EXPECTED_VERSION_ID=<VERSION_ID_NOVO> EXPECTED_DELIVERY_MODE=live npm run postdeploy:check
```
