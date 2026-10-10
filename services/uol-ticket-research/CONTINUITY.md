# Continuidade no Oracle e margem dos planos gratuitos

Esta etapa usa somente o Oracle e o Worker já existentes. Não cria recursos
Cloudflare, não migra contas e não altera resgates, OCR ou a sentinela pausada.
Os quatro destinos de ingressos continuam usando a fila e os recibos atuais.

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
essenciais antigas por 30 minutos. Ausência comprovada de Stories é saudável.
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
`duration` é GB*s, conforme introspecção real; armazenamento sem grupo é unknown.

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
real no Oracle permite `event`: push imediato mais consulta de segurança de
30 minutos (até 96 GETs/dia de segurança, mais eventos dentro do orçamento).

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
