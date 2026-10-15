# Continuidade no Oracle e margem dos planos gratuitos

Esta etapa usa somente o Oracle e o Worker já existentes. Não cria recursos
Cloudflare, não migra contas Cloudflare e não altera resgates, OCR ou a sentinela pausada.
Os quatro destinos de ingressos continuam usando a fila e os recibos atuais.

## Conta remetente WhatsApp no Oracle

Em 14/10/2026, a conta remetente foi substituída na sessão Baileys compartilhada
do Saquetto Drops. UOL, complemento Instagram, posts do X e envios manuais
compartilham essa sessão; UOL e Instagram chegam pelo gateway existente. O Mac só participou do pareamento;
a operação permanece no Oracle. URLs, tokens, destinos e chaves de idempotência
foram preservados; a segunda conta Cloudflare não foi utilizada.

Remetente e destinatário pessoal são identidades distintas. A rota de nome
legado `self` continua apontando para o contato pessoal original, enquanto o
botão ntfy abre a conversa com a conta remetente atual. Não trocar esse alias
para fazer o health passar. Antes de uma próxima troca, seguir o
[procedimento do remetente](../saquetto-drops/README.md#replacing-the-whatsapp-sender):
pausa também no arquivo de configuração, piloto desligado, backup consistente,
substituição somente de `auth` e verificação real dos destinos sem mensagem.

O gateway confirmou Ready/HTTP 200 e houve recibo de participante em uma entrega
normal após a troca. Isso não resolve os incidentes históricos do Worker, não
prova entrega a todos os grupos e não comprova Web Push do Instagram. Evidências,
versões e limites estão no [registro de publicação](ROLLOUT-2026-10-10.md#troca-do-remetente-whatsapp--14102026).

## Acesso administrativo para diagnóstico

Após autorização explícita em 14/10/2026, somente o `ADMIN_TOKEN` do Worker UOL
foi substituído. A cópia local recuperável fica em
`~/.config/uol-ticket-research/shadow-admin-token`, modo **0600**, dentro do
diretório **0700**. Esse arquivo não entra no Git nem é enviado aos serviços do
Oracle. Os tokens de ingestão, Analytics e da sentinela permanecem separados.

Use o valor somente em memória no header Bearer das rotas autenticadas de
diagnóstico. Não coloque o token em argumentos, URLs, stdout ou relatórios;
recuse redirects. Node `fetch` confirmou `/dashboard.json` e `/inventory` com
HTTP 200 e acesso sem token com 401. A checagem inicial por Python recebeu 403;
a diferença entre clientes não teve causa comprovada. Não repetir rotação por
falha de leitura: testar primeiro o token já salvo sem executar nova mutação.

As respostas contêm dados privados: publique apenas enums, contagens, horários
e classificações de erro. `sanitizeError` não remove credenciais; erros e
resumos brutos não são seguros para relatório. A janela de incidentes é de 12,
inventário de 300 e timeline de 20 eventos por oferta. Um evento `unknown`
histórico não prova pendência atual, e `last_detected_at` não prova evento novo.

Diagnóstico de 14/10 confirmou o incidente adicional no comentário do grupo
Telegram: resultado desconhecido em 13/10 às **15:47:01 UTC**, com erro de
resposta de rede e publicação principal confirmada às **15:45:08 UTC**. Não
há prova suficiente para marcar o comentário como enviado ou não enviado.
Preservar estado e exigir recibo independente antes de resolver ou reenviar.

## Alertas internos do Worker UOL

O Worker também envia alertas técnicos exclusivamente ao ntfy particular. Uma
outbox em metadata guarda eventos enumerados e tentativas antes do POST; nunca
copia títulos, IDs, detalhes ou erros privados para a mensagem. Até 20 tentativas
por 24 horas, cinco reservadas para críticos/recuperações, três por evento e
backoff mínimo de um/cinco minutos com `Retry-After`. A verificação interna é a
cada cinco minutos. É um orçamento separado dos 20 do supervisor Oracle.

Abertura e recuperação têm eventos independentes: uma recuperação rejeitada
continua pendente mesmo após o incidente ser resolvido. Timeout/crash depois da
reserva fica incerto, sem reenvio automático. Situação inalterada não repete
alerta. Incidentes anteriores ao transporte ntfy permanecem registrados e não
são migrados/reproduzidos. O heartbeat Instagram não usa mais fallback Telegram;
o supervisor Oracle mantém a responsabilidade por esses alertas.

O teto de 128 eventos vale somente para pendentes/reservados. Terminais mantêm
identidade e resultado por sete dias, sem ocupar essa capacidade ou liberar replay.
`notificationDiagnostics` no dashboard autenticado expõe apenas horários, motivos
enumerados e códigos HTTP válidos. Falhas antigas sem motivo seguem `unknown`;
não preencher retrospectivamente nem reenviar com base na ausência do cache ntfy.

Em 15/10, validação nativa workerd demonstrou a causa local do transporte:
`redirect: "error"` lança antes da saída. O request corrigido usa `manual` e trata
3xx como rejeição permanente, sem seguir destinos. Novos resultados distinguem
`request_invalid`, `fetch_invocation_error`, `fetch_rejected` e `response_invalid`;
nenhum erro livre é persistido e nenhum registro antigo é reclassificado.

Oscilações permanecem no mesmo incidente até recuperação estável: sucesso
afirmativo fresco, ao menos duas observações distintas e cinco minutos contínuos.
Silêncio, amostra antiga ou entrega desconhecida não comprovam recuperação.
Janela é durável; falha nova a zera e lacuna acima de dez minutos a reinicia.
Falha crítica nova e escalada para crítico são acionáveis imediatamente, uma vez
por episódio; resultado incerto da abertura não é reenviado.

A fila principal não libera `unknown` pelo tempo decorrido. É preciso recibo ou
resolução explícita com evidência. Backlog usado para priorizar o principal
considera envios elegíveis e sua própria idade, conservando contagens de
incidentes terminais para diagnóstico/readiness.

## Supervisão independente

`uol-instagram-supervisor.timer` executa a unidade oneshot a cada dois minutos,
sob o usuário `uol-instagram-monitor`, com teto de 48 MB. Não consulta Instagram
nem Analytics: lê unidades systemd, `loop-heartbeat.json`, `status.json`,
`storage-usage-state.json` e agregados de `push.sqlite`.

Configuração privada `supervisor.json` (0600), no diretório de estado 0700:

```json
{
  "ntfyUrl": "https://ntfy.sh/macXntfy-8130",
  "credentialExpiries": [
    {"name": "analytics-read", "expiresAt": "2026-11-09T00:00:00Z"}
  ]
}
```

Esse é o único destino operacional permitido. O registro de incidentes/outbox
fica em `supervisor-state.json`. A tentativa é persistida antes do POST; um
crash depois do envio resulta em `uncertain`, sem alegar entrega confirmada.
HTTP aceito significa aceitação pelo ntfy, não leitura no telefone.

Avisos são agrupados por execução, uma abertura e uma recuperação por incidente.
Recuperação exige evidência afirmativa; arquivo ausente/ilegível não recupera
incidente. Até 20 tentativas em 24 horas, com cinco reservadas para falhas
críticas/recuperação; até três tentativas por aviso. HTTP 429/5xx usa atrasos
mínimos de um e cinco minutos e `Retry-After` global durável. Timeout ambíguo não
é reenviado automaticamente. Mensagens não incluem sessões, IDs ou corpos HTTP.

Limiares: serviço inativo em duas verificações, loop sem vida por cinco minutos,
push sem conexão/vida por dez minutos, autenticação exigida, três falhas de
coleta sem sucesso por dez minutos, entrega estagnada por 15 minutos e métricas
essenciais antigas por 30 minutos. `lastPublishedAt` é verificado separadamente:
Analytics fresco não prova ingestão no Worker; publicação antiga por 30 minutos
gera incidente crítico próprio. Ausência comprovada de Stories é saudável.
Datas futuras além de 60 segundos não contam como evidência de vida.

O token Analytics tem avisos a sete dias, 48 horas e no vencimento; a configuração
não renova o token. Expiração de sessão Instagram depende de evidência da fonte,
nunca de não haver publicações. Intervalo superior a dez minutos sem execução do
supervisor gera aviso ao retomar, sem atribuir uma causa não demonstrada.

## Cotas e previsões

O observador mantém uma reserva durável e lock: no máximo duas consultas GraphQL
por rodada de 15 minutos (até 192/dia). A consulta global de reads/writes não
depende do detalhamento por projeto. Aliases adicionais com erro/truncamento
ficam desconhecidos; nunca viram zero. Namespace é atribuído a projeto somente
por `scriptName` comprovado; identidade ambígua fica sem atribuição.

`budgetState.metrics` contém valor, origem temporal, status, limite e previsão.
`lastDetails` e `dailyDetails` guardam projetos/tipos; `dailyMetricStatus` indica
se o histórico de cada dimensão foi reconfirmado. Requisições adaptive são
estimativas. Tipos DO não reconhecidos têm `billing_unknown`, com contagem bruta
preservada, sem converter mensagens WebSocket arbitrariamente em requisições.
`duration` é GB*s, conforme introspecção real. Armazenamento SQLite usa
`durableObjectsSqlStorageGroups`, confirmado por introspecção e consulta real
com o token existente. A soma dos máximos registrados por namespace é uma
estimativa conservadora dos dados observados, não tamanho instantâneo. Dados
ausentes/truncados seguem unknown; `durableObjectsStorageGroups` não serve como
fallback de SQLite. Valores antigos sem origem SQL comprovada são descartados
somente dessa dimensão, sem alterar as demais reservas de consumo.

Meta: menos de 70 mil writes por dia UTC completo. Atenção aos 70%; trabalho
opcional adiado aos 80% de qualquer dimensão conhecida. Limites configurados:
100 mil writes, cinco milhões reads, 100 mil requisições Workers/DO, 13 mil GB*s,
cinco bilhões de bytes de armazenamento DO. São limites compartilhados da conta.

Previsão só começa após uma hora de amostras contínuas e monotônicas. Usa a maior
taxa entre última hora e média desde meia-noite UTC. Duas previsões acima de 85%
adiam opcionais; duas abaixo de 75% liberam apenas suspensão por previsão.
Consumo real de 80% fica travado até novo dia. Storage é gauge, sem projeção diária.

O intake existente `/ingest-storage-usage` aceita o par adicional
`optionalWorkDeferred`/`optionalWorkReason`. Razões: `none`, `quota_actual`,
`quota_forecast`, `quota_metrics_stale`; falso exige `none`, verdadeiro exige
outra razão. Payload antigo continua válido. O par afeta somente manutenção;
descoberta principal, entrega e recibos conservam suas proteções. Mesma amostra
não regrava; alterar valor/hint com o mesmo horário é rejeitado.

## Retenção e evidência de Stories

Payloads criptografados concluídos duram sete dias; compactação troca apenas o
payload por `{}`, preservando hash, resultado, identificadores e sinais. Pendentes
nunca são compactados. Limites: cinco mil pendentes, 32 MiB de payloads, 100 mil
identidades. Aos 80% dos bytes, terminais mais antigos são compactados antecipadamente
em lotes atômicos de 50. Nenhum VACUUM no loop. Novos pacotes sem espaço não recebem
ACK; replays conhecidos continuam aceitos. Pressão recuperável mantém compactação,
pausa a conexão e reconecta mesma UAID/canal abaixo de 80%; limites de pendência ou
identidade exigem revisão. O estado expõe contagens e motivos sanitizados.

`story_coverage` guarda primeira observação, gatilho, correlação e recibos por
destino. Baseline/lacunas não contam na taxa de sucesso. Story ID é correspondência
exata; sinal genérico é incerto. Sem sinal após dez minutos é
`without_corresponding_push`, não diagnóstico causal. Sinal exato tardio corrige
a correlação. Histórico local acompanha a retenção de 30 dias da outbox.

Estado idêntico não regrava SQLite/status a cada dois segundos. Liveness usa
checkpoint local de 60 segundos; reservas e transições continuam comprometidas
antes da rede. Estrutura Instagram desconhecida permanece desconhecida; diagnóstico
usa somente contagens/flags da próxima coleta já agendada, sem chamadas adicionais.

`pilot` conserva 120 segundos e os limites anteriores. Somente prova de Story
real com ID correspondente no Oracle permite `event`: a prova deve ser gravada
até dez minutos depois do sinal e não pode estar no futuro. Sinal genérico
continua disparando coleta, mas não libera sozinho a promoção. Prova exata válida
permanece válida após expiração do Story/reinício. No modo `event`: push imediato mais consulta de segurança de
30 minutos (até 96 GETs/dia de segurança, mais eventos dentro do orçamento).

Desde 15/10/2026, por decisão explícita após aviso do Instagram, a configuração
operacional é `push_only`: nenhuma coleta periódica, inclusive após reinício,
backoff vencido ou desconexão. Apenas sinal pendente libera coleta, respeitando
os mesmos limites e reservas. O estado informa `activePollPeriodSeconds: 0`.
Isso não valida a recepção push nem altera o gate do modo `event`. Sem polling
independente não é possível estimar os Stories que deixaram de chegar por push.
Reativar qualquer polling exige nova decisão do usuário.

Cada lote push tem até quatro reservas em dez minutos, persistidas antes da rede.
Um snapshot vazio/antigo não conclui sinal exato; um sinal genérico continua incerto
e exige duas observações separadas por sessenta segundos com Story novo. Limite ou
prazo encerra só aquele lote como `unresolved`, sem polling posterior. Restart,
novo sinal e erros não reiniciam o orçamento; os backoffs existentes prevalecem.
O supervisor alerta esgotamento e só registra recuperação com resolução exata.

Diagnóstico do receptor usa `receiver_diagnostics`, tabela aditiva com enum fechado,
contagens e horários. Ela distingue rejeição anterior à persistência, decryption,
classificação, replay e envio local de ACK. Não guarda corpo/IDs/URLs, não apaga
eventos nem recupera contagens anteriores à instalação. O supervisor alerta falhas
recentes uma vez; silêncio não comprova recuperação e nenhum teste é enviado aos
grupos. Contadores de frames incluem handshake/pong, e ACK local não é recibo remoto.
O supervisor guarda o último contador de lotes esgotados observado para capturar
falhas ocorridas entre verificações. Rejeição só se recupera após nova persistência,
ACK local e sinal; a evidência posterior não vence enquanto aguarda a próxima
verificação. Uma conexão SQLite de diagnóstico é fechada ao terminar a leitura.

Manutenção do Worker libera sua flag de execução inclusive quando preparação ou
gravação de metadata lança por cota. Isso evita bloqueio permanente após o reset;
não resolve nem altera incidentes históricos de entregas desconhecidas.

## Validação, publicação e rollback

Testes focados: supervisor, monitor, storage usage, push sob Node 22 e integração
do Worker. Validar uma mensagem identificada como teste somente no ntfy particular;
nenhuma mensagem sintética nos canais de ingressos. CI e release guard do Worker
continuam obrigatórios; `/livez` não substitui `/readyz`.

Antes da instalação, backup privado consistente dos dois bancos e código no
Oracle. Rollback troca código/configuração, preservando bancos e eventos recentes.
Após compactação, manter a correção de capacidade: versão antiga volta a contar
tombstones no teto global de cinco mil. Não restaurar backup sobre novas entregas.

Aceitação de produção exige três dias UTC completos, métricas suficientes,
meta de writes, margem nas demais dimensões e ausência de regressão/duplicação.
Se não houver Story novo, consumo pode ser avaliado, mas push continua pendente.
Revisão do Codex é complementar; serviços e alertas do Oracle independem do Mac.

Relatório offline, sem rede nem gravações: `python3 usage_report.py --state-dir
/caminho/privado/do/estado --day YYYY-MM-DD`. Lê somente
`storage-usage-snapshots/*.json` e imprime JSON com agregados e nomes de scripts;
nunca namespace/account ID ou payload. `consumption_verified` exige dia UTC
concluído, amostras até 30 minutos das duas bordas, nenhuma lacuna superior a
30 minutos, métricas conhecidas e confirmação posterior do total daquele dia.
Snapshots de outros dias não contam na cobertura; desconhecidos e dados inválidos
não viram zero. `incomplete` e `coverage_insufficient` mantêm a aceitação pendente.
Os snapshots são anteriores à ingestão: esse relatório não comprova guard no
Worker, entrega, push, latência, duplicatas nem a aceitação completa de produção.
O resultado de cada dimensão é independente: `rowsWritten.consumptionVerified`
e `writeTargetMet` podem comprovar a meta de writes mesmo com status global
`coverage_insufficient` por billing DO desconhecido ou outra margem pendente.
`storageBytes` só fica conhecido com origem SQL comprovada no snapshot; source
legacy/ausente fica desconhecido no relatório, sem apagar o registro original.
