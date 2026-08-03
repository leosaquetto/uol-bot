# Registro da migração do Telegram para Cloudflare

> Este arquivo combina recibos históricos de produção com o desenho da próxima
> versão. A seção de 03/08/2026 descreve mudanças locais e não deve ser lida como
> prova de deploy; somente Version ID novo + `postdeploy:check` provam ativação.

## Objetivo

Eliminar a dependência do Mac ligado e reduzir a latência entre a publicação de
uma oferta no Clube UOL e a notificação no Telegram.

## Arquitetura do Worker

O Worker `uol-telegram-shadow-pilot` preserva o nome do piloto para reutilizar o
mesmo Durable Object e o baseline já validado. A execução é serializada por uma
única instância porque a listagem global do Clube UOL é uma única unidade de
coordenação.

O estado SQLite contém:

- ofertas conhecidas e seus detalhes;
- chaves de deduplicação;
- decisão do canal principal e do canal 2;
- IDs, horários, tentativas e erros de cada entrega;
- sincronização de esgotamento por canal;
- até 240 eventos, falhas, recuperações e amostras saudáveis de execução;
- baseline e início do modo live;
- outbox por destino com geração de modo, próxima tentativa, in-flight,
  resultado incerto, dead letter e quarentena;
- estado anterior ao esgotamento e sincronização de reabertura.

Antes de inserir um card, o Worker compara as identidades canônicas do endereço.
Além do slug completo, é usada a combinação estável
`parceiro + código interno da oferta`. Essa segunda chave protege contra
alterações de codificação como `tênis/tnis`, `cardápio/cardpio` e
`grátis/grtis` sem confundir ofertas de parceiros diferentes.

A canonicalização foi alinhada ao consumidor legado: percent-encoding,
mojibake, remoção de diacríticos, variantes `de`/`João` e correções conhecidas
como `seleo/selecao`, `ps/pos`, `at/ate` e `grtis/gratis`. A deduplicação
também conserva as chaves de conteúdo estrita/solta e o bloqueio recente por
título + validade durante sete dias.

Dois alarmes independentes evitam que trabalho secundário atrase a descoberta.
O primeiro consulta somente a API e envia o canal principal. O segundo executa
HTML, webhook, canais secundários, comentários, esgotamento e reabertura. HTML
vazio ou suspeitamente pequeno falha de forma segura e não produz esgotamentos.

O modo operacional também é persistido no Durable Object e pode ser alterado
imediatamente pela rota autenticada `POST /mode`. Isso evita depender da
reciclagem de uma instância aquecida após um deploy e permite contenção
`live -> shadow` sem aguardar uma nova publicação.

## Entrega ao Telegram

O Worker utiliza a Bot API diretamente. No fluxo urgente descoberto pela API:

1. valida e grava a decisão no SQLite;
2. tenta `file_id`, URL e upload da foto sem ultrapassar 60 segundos desde a
   primeira detecção;
   mutações param 38 segundos antes para reservar timeout e reconciliação;
3. envia texto sem preview somente quando o prazo expira e persiste a confirmação;
4. despacha todos os principais da rajada com concorrência limitada;
5. processa Discord em alarme secundário antecipado e reutiliza seu proxy de
   thumbnail quando URL/upload do UOL falham;
6. para campanhas de ingresso, usa `copyMessage` para o canal 2 depois do
   upgrade de imagem, criando uma mensagem independente e editável;
7. uma falha secundária não apaga o sucesso do canal principal;
8. novas tentativas processam somente o destino ainda pendente.

Se a foto aparecer depois do texto, `editMessageMedia` substitui o conteúdo da
mesma mensagem por `InputMediaPhoto` com a legenda completa. O `message_id` é
preservado e nenhuma segunda publicação é criada.

Um webhook opcional `DISCORD_IMAGE_CACHE_WEBHOOK_URL` permite usar canal privado
como cache para ofertas comuns. Nada é publicado no canal visível de ingressos;
o cache só é acionado após falha das fontes diretas de imagem.

As legendas utilizam HTML escapado, link canônico, validade, localização quando
disponível e as hashtags relevantes. Campanhas de ingresso não são silenciosas.

## Esgotamento

Uma oferta entregue passa a ser candidata a esgotamento somente quando:

- foi decidida nos últimos três dias;
- desapareceu de uma listagem saudável;
- acumulou pelo menos duas ausências;
- permaneceu ausente por no mínimo 15 minutos.

Depois da confirmação, o Worker usa `editMessageCaption` para fotos ou
`editMessageText` para mensagens sem imagem. Principal e canal 2 possuem
confirmações e retries independentes. `message to edit not found` é terminal no
esgotamento; na reabertura, a oferta é republicada e o novo `message_id` passa a
ser a referência durável. Envio substituto ambíguo não é repetido automaticamente.

## Baseline e corte

O baseline inicial de 30/07/2026 registrou 48 ofertas e não enviou nenhuma
mensagem. As 12 ofertas recentes observadas pelo Mac estavam presentes no
inventário remoto.

Na promoção:

- as ofertas do baseline permaneceram bloqueadas;
- candidatos registrados durante o modo sombra não são reenviados
  retroativamente;
- somente decisões criadas depois do início do modo `live` entram na fila;
- o workflow legado foi convertido em fallback manual;
- o LaunchAgent do Mac foi descarregado;
- o Worker do Discord permaneceu inalterado.

## Segurança

- tokens e IDs de chat são secrets do Worker;
- valores não são expostos nas rotas públicas de liveness/readiness, logs ou
  repositório;
- a cópia inicial dos secrets foi feita por um workflow temporário;
- o workflow e a credencial temporária de Cloudflare foram removidos após o
  sucesso;
- rotas mutáveis exigem `ADMIN_TOKEN`;
- a cópia local do `ADMIN_TOKEN` fica no Chaves do macOS, no serviço
  `uol-telegram-cloudflare-worker-admin-token`;
- logs contêm somente eventos, contagens e erros sanitizados.

## Validação

- testes unitários de coleta, deduplicação, validade e filtros;
- testes de legenda HTML e marcação de esgotamento;
- testes do fallback `sendPhoto` para `sendMessage`;
- testes da cópia independente ao canal 2;
- teste real do transporte no canal principal e no canal 2;
- geração de tipos pelo Wrangler;
- dry-run do pacote;
- migração SQLite remota;
- saúde e cadência do alarme verificadas em produção.

## Ativação confirmada

- Data: 30/07/2026
- Início do modo live: `2026-07-30T06:38:57.696Z`
- Version ID: `447a4e10-bf21-4462-9969-6a5056f0aaa0`
- Startup remoto: 6 ms
- Inventário no corte: 48 ofertas de baseline
- Pendências no corte: zero
- Erros de entrega no corte: zero
- Teste real: principal e canal 2 confirmados
- Workflow automático legado: removido
- LaunchAgent `com.leosaquetto.uolmonitor`: descarregado

## Incidente de canonicalização de 30/07/2026

Às `07:05Z`, a listagem alternou endereços com letras acentuadas perdidas para
endereços corrigidos. Como o baseline inicial armazenava apenas o slug visto
naquele momento, 20 aliases foram inicialmente classificados como novos.

- oito mensagens chegaram ao canal principal antes da contenção;
- nenhuma foi encaminhada ao canal 2;
- o modo foi alterado imediatamente para `shadow`;
- as 12 decisões ainda não enviadas foram bloqueadas;
- os 20 pares foram reconciliados por `parceiro + código interno`;
- os oito registros enviados foram preservados para permitir edição futura de
  esgotamento;
- após o reparo: zero pendências, zero aliases ativos e zero erros de entrega.

O caso real foi convertido em testes de regressão para `grtis/gratis`,
`cardpio/cardapio`, `tnis/tenis`, mojibake e variantes do consumidor legado.

### Estado após a correção

- Version ID: `c6b4de31-2955-4333-99fd-92c80fc09d6b`
- modo persistido: `live`
- listagem saudável: 48 ofertas
- identidades ativas: 48
- aliases ativos: zero
- pendências de entrega: zero
- erros de entrega: zero
- primeiro alarme após a retomada: `48 vistas / 0 novas / 0 envios`

## Rollback

1. executar `POST /mode` com `{"mode":"shadow"}`;
2. publicar a versão protegida;
3. executar `bot leouol scraper - fallback manual`;
4. se necessário, recarregar
   `~/Library/LaunchAgents/com.leosaquetto.uolmonitor.plist`.

O Discord não participa desse rollback.

## Consolidação de baixa latência de 31/07/2026

Registro histórico, substituído pela arquitetura API-first de 03/08/2026.

O coletor passou a ser a fonte única para Telegram e Discord:

- alarme reduzido de 30 para 15 segundos;
- uma única leitura e uma única decisão durável por oferta;
- Telegram e Discord disparados em paralelo antes do enriquecimento para
  ingressos novos; a legenda é completada por edição logo depois;
- Discord preserva o embed aprovado com thumbnail;
- o coletor Discord anterior ficou com `COLLECTOR_ENABLED=false`, sem alarme;
- ofertas históricas foram marcadas como já entregues no Discord durante a
  migração, impedindo replay.

O histórico completo do Telegram foi recuperado por webhook:

1. o canal recebe thumbnail e legenda compacta;
2. o Telegram encaminha automaticamente a publicação ao `LeoUOL Chat`;
3. o webhook valida `X-Telegram-Bot-Api-Secret-Token`;
4. `forward_origin.message_id` é associado ao `main_message_id` durável;
5. a descrição completa é formatada e enviada como resposta nessa discussão;
6. cada chunk confirmado é persistido para que retries não o repitam.

Na ativação histórica de 31/07 o registro usou `drop_pending_updates=true`. A
versão atual preserva pendências com `false`, pois o consumidor é idempotente e
usa o mesmo fluxo para reconciliar uma publicação principal de resultado
incerto. Para rollback a `getUpdates`, o webhook deve ser removido primeiro.

### Estado da ativação

- Worker central: `uol-telegram-shadow-pilot`
- Version ID final de baixa latência: `df550012-fe56-4c07-aab4-ea8f81b733af`
- intervalo: 15 segundos
- grupo vinculado: `<GRUPO_COMENTARIO_ID>`
- Discord consolidado: configurado
- webhook Telegram: registrado
- ofertas novas durante a migração: zero
- reenvios durante a migração: zero
- erros de entrega durante a migração: zero

## Encerramento dos consumidores legados e otimização de escrita

Em 31/07/2026, os dois workflows UOL foram desativados na API do GitHub e
movidos para `.github/workflows-archive/`. O LaunchAgent foi descarregado e o
plist passou para `~/Library/LaunchAgents.disabled/`.

O update periódico de `last_seen_at` também foi removido. Cards conhecidos só
geram `UPDATE` quando URL, título, categoria, imagens ou estado de ausência
mudam. Isso eliminou cerca de 276 mil escritas redundantes por dia na cadência
de 15 segundos.

- versão otimizada: `bcfe236f-89ea-4e94-9c23-56b3e605bd81`
- invocações projetadas do alarme: 5.760/dia
- escritas SQLite estáveis projetadas: aproximadamente 17.280/dia
- aliases ativos após a mudança: zero
- erros de entrega após a mudança: zero

### Retenção automática

A manutenção diária remove ofertas terminais que não estão mais na listagem e
tenham mais de 30 dias. O teto adicional preserva no máximo 300 registros
terminais. Ofertas ainda visíveis e estados com entrega pendente são protegidos,
portanto a limpeza não pode apagar um alerta antes do envio nem transformar um
card antigo ainda publicado em uma oferta nova.

## Métricas e alertas operacionais de 01/08/2026

O mesmo Durable Object passou a medir a latência ponta a ponta e guardar
incidentes numa tabela SQLite própria. A janela de 24 horas começa na ativação,
evitando que comentários históricos recuperados horas depois distorçam p50 e
p95. Cada confirmação de Discord, Telegram, canal 2 e comentário registra o
instante efetivo de conclusão da chamada.

Alertas cobrem autorização da API de ingressos, três falhas consecutivas da API
ou do scan, webhook incorreto/acumulado e ingresso novo sem foto ou comentário
após três minutos. A chave durável impede duplicatas, o cooldown é de seis horas
e somente incidentes críticos geram mensagem de recuperação. A falha do próprio
alerta é persistida e nunca interrompe a coleta.

- versão publicada: `b57d1754-c915-40d2-8ef5-78eeb7ff71b2`
- intervalo preservado: 15 segundos
- migração interna: esquema v6 (`incidents`)
- API na verificação: 3 ingressos, erro vazio
- webhook: URL correta, zero updates pendentes
- observação: três ciclos consecutivos, zero falhas e zero incidentes ativos

## Fontes, imagens, painel e autenticação de 01/08/2026

O esquema v7 adicionou comparação durável entre API e HTML, cache de imagens do
Telegram e saúde independente por estratégia de entrega. O Worker registra
somente campanhas de ingresso na comparação, preserva o primeiro instante visto
em cada fonte e calcula vencedor, delta, p50, p95 e máximo por oferta.

Na primeira amostra real pareada, a API descobriu `2 INGRESSOS: 02/08 Shopping
Cidade SP` 934 ms antes da listagem. Duas ofertas do Teatro J. Safra estavam
somente na API, comportamento compatível com a listagem pública ser a autoridade
de disponibilidade/esgotamento. Por isso, diferença parcial não é incidente:
somente ausência total de sobreposição persistente é tratada como divergência.

O Telegram agora guarda o maior `file_id` devolvido por `sendPhoto`. Uma imagem
já conhecida deixa de depender novamente do servidor do Clube, do Discord ou de
upload. Falhas de `file_id`, URL remota, proxy do Discord e upload alimentam
circuitos separados; três falhas abrem a estratégia por dez minutos. O estado e
o cache aparecem no painel e em `/dashboard.json` sem expor identificadores
sensíveis.

O painel autenticado em `/dashboard` é renderizado no servidor, não executa
JavaScript e atualiza a cada 30 segundos. Ele mostra ofertas recentes, fonte
vencedora, latências de Discord/Telegram/comentário, circuitos de imagem,
incidentes, saúde de autenticação e consumo estimado. `/dashboard.json` oferece
o mesmo snapshot para diagnóstico automatizado. Ambas as rotas retornam 401 sem
Bearer ou HTTP Basic válido.

O alarme também ganhou autorreparo: um agendamento ausente ou atrasado por mais
de dois intervalos é rearmado para o próximo ciclo. Isso recuperou em produção
um alarme antigo que estava vencido, e os scans voltaram à cadência de 15–17 s.

### Descoberta temporária de autenticação e eliminação do login pessoal

Uma rota administrativa temporária `POST /auth-discovery` testou combinações de
cabeçalhos e devolveu somente status HTTP, contagem de ofertas, formato do token,
expiração e nomes dos claims. Nenhum token ou valor de claim saiu do Worker. O
ensaio real confirmou que `Authorization` técnico sozinho retorna HTTP 200,
enquanto token pessoal sozinho e requisição sem credencial retornam HTTP 401.
Portanto, `X-Authorization`, login UOL e senha não participam da leitura das
ofertas. A rota foi removida depois da descoberta para reduzir a superfície
administrativa permanente.

O fluxo automático de senha e todo Browser Rendering foram retirados. Nenhum
navegador remoto participa de descoberta, enriquecimento ou envio. A credencial técnica atual expira em
23/10/2026 e gera alerta operacional 14 dias antes; ainda assim, sua expiração
não interrompe a coleta porque o Worker passou a consultar em paralelo duas
fontes totalmente públicas: a listagem geral e a categoria dedicada
`/?categoria=ingressosexclusivos&order=new`.

Essa categoria pública funciona como fonte permanente e sem credencial. Na
versão histórica, a API era tratada como acelerador e fonte estruturada
adicional. A revisão local de 02/08 a promove a fonte primária de publicação; o
HTML continua como fallback degradado e autoridade de disponibilidade, sem ser
confirmação anterior ao envio. A sonda administrativa temporária não permanece
exposta.

## Revisão local API-first de 03/08/2026 (publicação pendente)

- a API completa roda sozinha em todos os alarmes de 15 segundos;
- uma novidade é validada, deduplicada e enviada antes de qualquer HTML;
- a entrega principal tenta foto imediatamente e espera no máximo 60 segundos;
- após o prazo envia texto sem preview; foto tardia edita a mesma mensagem para
  foto + legenda via `editMessageMedia`;
- HTML geral e exclusivo reconciliam a cada 60 segundos, ou imediatamente se a
  API falhar/voltar vazia;
- polling API e manutenção possuem Durable Objects e alarmes independentes;
- polling permanece em 15 segundos; manutenção passa a 60 segundos;
- telemetria frequente usa snapshots JSON e observações respeitam janela de
  toque de 15 minutos;
- ciclos `no_change` são amostrados no histórico a cada 15 minutos; eventos,
  falhas e recuperações continuam imediatos;
- cada handler periódico rearma o alarme uma vez e a concorrência de entrega é 6;
- leituras SQLite reais são acumuladas por dia UTC; manutenção é cortada primeiro
  e o polling adapta a cadência antes do limite de 5 milhões;
- os dois alarmes se rearmam antes da leitura pesada e retomam após o reset;
- o orçamento conservador com 48 cards por fonte é 57.920 gravações/dia,
  incluindo reserva de 20.000, abaixo do limite gratuito de 100.000;
- o alarme crítico não chama HTML, webhook, Discord, comentários ou esgotamento;
- rajadas priorizam todos os Telegram principais com concorrência limitada;
- principal, canal 2 e Discord guardam estados `unknown` independentes;
- o forward automático do Telegram reconcilia o ID de um principal ambíguo;
- sem forward em 30 segundos, o principal é tentado novamente automaticamente;
- `POST /resolve-delivery` permite confirmar `sent`/`not_sent` antes de retry;
- dead letters não criam head-of-line blocking e dependências impossíveis não
  ficam em backoff eterno;
- reabertura restaura `partial_delivery` quando havia secundários pendentes;
- schema local: v17;
- não há Version ID nem recibo de produção nesta seção porque não houve commit,
  push ou deploy nesta revisão.

### Retenção adicional

- observações de fonte: 30 dias;
- cache de `file_id`: 90 dias sem uso e no máximo 500 entradas;
- ofertas terminais: 30 dias e no máximo 300, preservando visíveis e pendentes;
- execuções: até 240 eventos e amostras.

### Último recibo de produção anterior a esta revisão

- esquema remoto preservado: v9;
- Version ID: `de37c8e7-3bba-499b-82b7-d336edc9f624`;
- testes históricos: 56 aprovados;
- pacote: tipos atualizados e deploy dry-run aprovado;
- painel HTML/JSON autenticado: HTTP 200; sem autenticação: HTTP 401;
- sonda: `Authorization` técnico sozinho HTTP 200; token pessoal sozinho e
  nenhuma credencial HTTP 401;
- ciclo real: 48 ofertas na listagem geral, 1 na categoria pública exclusiva e
  1 na API; `no_change`, zero envios, zero erro de API e zero incidente ativo;
- autenticação: senha pessoal fora do fluxo; expiração técnica exposta apenas
  como data sanitizada em `/dashboard.json`;
- esta seção não comprova a revisão local de 03/08; um novo Version ID e um
  ciclo real serão exigidos depois de eventual deploy autorizado.
