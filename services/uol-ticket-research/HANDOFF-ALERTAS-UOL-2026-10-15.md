# Handoff — corrigir os alertas técnicos do UOL

## Pedido e entrega esperada

O usuário pediu continuar em uma conversa nova e arquivar a anterior. A prioridade
agora é concluir o diagnóstico e a correção dos alertas ntfy, com prova do fluxo,
sem voltar ao ciclo de apenas revisar, listar falhas e esperar outro “corrija”.
Mantenha a investigação restrita. Leia os arquivos relacionados, não reindexe o
repositório. Use testes dirigidos e o CI obrigatório. Commit, push e deploy estão
autorizados após validação, apenas para arquivos desta tarefa. Não peça novamente
permissão já concedida. Decisões sensíveis e publicação ficam com o agente principal.

## Repositório e concorrência

- Projeto: `/Users/leosaquetto/Developer/GitHub/uol-bot`, branch `main`.
- No handoff, `git status --short` estava limpo antes de criar este documento.
- HEAD então era `fca330f29 Add MacSend WhatsApp group mirror transport`, mudança
  de outro trabalho. Preserve-a. Não reverta nem redesenhe WhatsApp/MacSend/Drops.
- Confira o status antes de editar; não sobrescreva mudanças concorrentes.
- Não use Superpowers nem Sol Advisor sem pedido explícito do usuário.

## Produção confirmada na última leitura desta conversa

Worker existente: `https://uol-telegram-shadow-pilot.leosaquetto.workers.dev`.

- Última versão UOL publicada por esta conversa:
  `1c4e3b74-04f4-44ad-a2d8-f3387d7ae187`, código `ad77b6045`.
- CI aprovado do código:
  `https://github.com/leosaquetto/uol-bot/actions/runs/38718535459`.
- `/livez` 200; `/readyz` 503. Alarmes, descoberta, manutenção, configuração e
  budgets saudáveis. Contagens históricas preservadas: criticalIncidents 3,
  deadLetters 2, unknown 4, blockedConfiguration 0, maintenanceDeadLetters 1.
- Não declarar Ready enquanto os gates falham. Não limpar/resolver/reprocessar
  incidentes ou entregas para fazer o diagnóstico ficar verde.
- Dashboard autenticado: **20 alertas ntfy incertos e 4 pendentes**. Entre os
  últimos 20 resultados: 15 `network_error`, 5 `unknown` legados. Sem HTTP de
  aceitação observado nesses registros. Essa leitura é uma fotografia, não um
  estado atual garantido na próxima conversa.
- Última tentativa listada nessa fotografia: 15/10/2026 às 18:02:39.389 UTC.
- Limite implementado: 20 tentativas do Worker por 24 horas, cinco reservadas a
  críticos/recuperações, até três tentativas por evento. Incerteza não é reenviada.
  O supervisor Oracle tem orçamento próprio; não confundir os dois contadores.
- Oracle: métricas publicadas há cerca de nove minutos e loop recente. Serviços
  monitor Instagram, receptor push, supervisor, Drops e gateway ativos na checagem
  anterior. Atividade de processo não prova entrega nem recepção de Story.

## Duas pendências efetivas

### 1. O transporte ntfy ainda não tem causa demonstrada

`src/operations-notify.js` tem um `catch` amplo que converte qualquer exceção em
`network_error`. Isso inclui erro local síncrono de invocação/validação e erro ao
tratar a resposta, além de rejeição de rede. Portanto, **não afirme que DNS, TLS,
Cloudflare ou ntfy são a causa**. A descrição anterior como falha de rede era
mais forte que a evidência disponível.

Leitura estática não encontrou causa comprovada: headers fixos ASCII, body de
texto válido, AbortController local e execução aguardada. A invocação livre de
`fetchImpl` também existe no transporte Telegram funcional.

Lacuna dos testes: usam `vi.stubGlobal("fetch")`, sem exercitar validação nativa
do Request/fetch no runtime Cloudflare. Caminho inicial sugerido:

1. Reproduzir com fetch nativo e saída interceptada no runtime de teste, sem
   acesso externo. Verificar URL, headers, redirect e sinal de abort.
2. Distinguir etapas e categorias de erro com allowlist fixa. Nunca armazenar ou
   imprimir mensagem bruta de erro, URL privada, corpo de resposta ou credenciais.
3. Provar a causa antes de escolher a correção. Se for necessário instrumento
   adicional de produção, mantenha-o autenticado, restrito e sem efeitos de envio.
4. Validar o transporte corrigido com uma única mensagem claramente identificada
   como teste **somente no ntfy particular**, dentro da autorização já concedida.
   Não repetir eventos incertos antigos como teste.

Uma leitura delimitada do cache de `ntfy.sh/macXntfy-8130`, do Mac e do Oracle,
retornou HTTP 200 e nenhuma mensagem com Title `UOL bot - aviso tecnico` na janela
consultada. Isso **não comprova não entrega**, nem confirma o caminho de saída do
Worker. Não reenviar ou resolver os 20 incertos com base nessa ausência.

### 2. Falta agrupar/estabilizar os alertas em oscilação

`processOperationalHealth` cria UUID novo em cada reabertura e recupera na primeira
amostra sem sinal. Não há agrupamento de ocorrências próximas ou estabilização.
Reprodução isolada: dez pares abertura/recuperação warning consomem as vinte
tentativas e adiam um crítico seguinte. A recuperação usa a reserva privilegiada,
conforme o contrato atual. Isso não comprova que os eventos reais foram oscilações.

Implemente agrupamento/estabilização restrito ao fluxo técnico, mantendo:

- primeira falha crítica acionável, recuperação com evidência afirmativa;
- estado durável e retomada após reinício;
- situação inalterada silenciosa e limite de tentativas preservado;
- nenhuma duplicação, reabertura artificial ou reenvio de resultados incertos;
- pendências existentes, Retry-After e reservas anteriores ao POST.

## Correções já concluídas: não refazer

- Telegram principal `unknown` não é liberado após trinta segundos. Exige recibo
  ou resolução explícita com evidência independente.
- Prioridade da fila usa principal realmente elegível e sua própria idade.
- Ranking SQL anterior ao LIMIT respeita alvos permitidos pelo orçamento,
  inclusive `targetNames=[]`, exceções recentes sob quota e até 96 prioridades
  vinculadas uma vez via `json_each`.
- Terminais de alertas não ocupam o teto de 128 pendentes/reservados; conservam
  identidade por sete dias. Fila cheia não impede sua drenagem.
- Abertura/recuperação são eventos duráveis separados. Recuperação rejeitada não
  some apenas porque o incidente virou resolved. Timeout/crash permanece incerto.
- Retry-After é global ao destino e não é encurtado para uma hora.
- Alertas técnicos não usam mais Telegram/Discord. Destino único:
  `https://ntfy.sh/macXntfy-8130`.
- Diagnóstico autenticado tem contagens, horários, motivo enumerado e HTTP válido,
  sem preencher causas antigas retrospectivamente.

## Arquivos de partida e validação

Base do Worker: `cloudflare-workers/uol-telegram-shadow-worker/`.

- `src/operations-notify.js`: formatação/POST ntfy e classificação do resultado.
- `src/operations-outbox.js`: reservas, limite, dedup, retenção e Retry-After.
- `src/worker.js`: `enqueueOperationalAlert`, `drainOperationalAlerts`,
  `processOperationalHealth`, `operationsNotificationDiagnostics`.
- `test/operations-notify.test.js`, `test/operations-outbox.test.js`.
- `test-worker/operations-notify.integration.test.js`.
- `test-worker/queue-budget.integration.test.js`: regressões já aprovadas da fila.
- README do Worker e `services/uol-ticket-research/CONTINUITY.md`.
- Início de `services/uol-ticket-research/ROLLOUT-2026-10-10.md`.

Última correção teve 13 testes unitários de outbox, 9 de integração de alertas e
6 da fila aprovados localmente; o CI completo passou. Um CI intermediário detectou
retorno `deferred` ausente; foi restaurado sem enfraquecer o teste.

Publicação obrigatória pelo script `npm run release` do Worker: main limpa e
sincronizada com origin/main, CI verde para o SHA exato, Node 22 no CI, guard de
fonte e Wrangler local. Não contornar os gates ou publicar outra conta/recurso.
Versão do DO pode demorar a convergir após deploy: faça uma checagem posterior
limitada se a primeira mostrar versões diferentes. Não repetir deploy por isso.

## Acessos protegidos: usar sem expor

- Admin Worker: `~/.config/uol-ticket-research/shadow-admin-token`, 0600,
  diretório 0700. Token rotacionado com autorização em 14/10. Não rotacionar outra
  vez, copiar para Oracle, Git ou logs, nem confundir com ingestão/sentinela.
- Usar Node fetch, Bearer só em memória, `redirect:error`, timeout <=10s.
- Diagnóstico: GET `/dashboard.json`; se realmente necessário,
  GET `/inventory?limit=300`. Uma leitura por rota por rodada. Não imprimir JSON
  bruto, IDs, chaves de incidentes, títulos, mensagens, summaries ou erros livres.
- SSH Oracle: `~/.config/uol-ticket-research/server.json`, campos `sshTarget` e
  `identityFile`. Não imprimir destino, usuário, IP ou chave.
- Estado Oracle `/var/lib/uol-instagram-monitor`, código
  `/opt/uol-instagram-monitor`, usuário `uol-instagram-monitor`.
- Ler apenas estados/agregados existentes: `storage-usage-state.json`,
  `status.json`, `loop-heartbeat.json`, `supervisor-state.json`, snapshots e
  metadados agregados dos SQLite. Não copiar bancos/sessões/payloads privados.
- Não repetir GraphQL: observador já executa a cada quinze minutos no Oracle.

## Restrições e pendências fora do foco

- Sem serviços pagos, recursos Cloudflare novos ou migração de conta.
- Preservar autenticação, filas, recibos, bancos, sequências, sessões e journals.
- Sem resgates, OCR, reativação da sentinela ou envio/reenvio sintético em grupos.
- Instagram foi colocado em **push_only**, intervalo zero por decisão explícita
  após aviso de automação. Não reativar polling nem fazer GET/login extra.
- Web Push real de Story **ainda não foi comprovado**. Mensagens anteriores
  chegaram por polling, não servem como prova de push. Recepção de socket/handshake
  também não é prova. Essa validação é separada da correção ntfy.
- Automação de validação de 72h está pausada por pedido do usuário. Pesquisa
  pública diária está ativa até 30/10, com limite próprio; não alterar nenhuma
  para validar esta correção. Arquivar o chat anterior não é autorização para
  reativar ou duplicar suas automações.
- Incidentes históricos de entrega não devem ser resolvidos/retried sem recibo
  independente. A mudança de código não equivale a reparo dos registros antigos.

## Critérios de conclusão desta continuação

1. Causa do erro de transporte demonstrada, ou limitação exata documentada sem
   inventar diagnóstico; correção da causa quando acessível.
2. Validação nativa local do request, agrupamento/recovery/restart/limites com
   fixtures e CI completo do SHA publicado.
3. Prova de aceitação de um novo teste técnico no ntfy particular, sem confundir
   HTTP aceito com leitura no telefone e sem reenviar incertos antigos.
4. Conferir liveness, versão, descoberta e orçamento; readiness histórico deve
   continuar descrito com precisão. Nenhuma regressão nos destinos de ingressos.
5. Handoff final curto: resultado, arquivos, validações, URL e pendências reais.

Comunique só mudança de hipótese, bloqueio ou resultado. Não faça auditoria
recursiva ampla, novos planos genéricos ou listas de falhas sem encaminhar a
correção já autorizada. Se esbarrar em limite real de acesso/infraestrutura,
registre o que foi comprovado e a próxima ação mínima.
