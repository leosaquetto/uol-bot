# Operação do Consumer (BOT LEOUOL) — Alertas de Falha

## Quando considerar incidente

Uma execução do workflow `.github/workflows/bot_leouol_consumer.yml` deve ser tratada como incidente quando o step **`processar pending offers`** falhar.

A partir desta configuração:
- o job passa a falhar de fato (sem `|| true`), marcando a run como **failed** no GitHub Actions;
- `commit` e `push` só executam em caso de sucesso do processamento;
- um step de diagnóstico (`if: failure()`) publica no log:
  - conteúdo de `status_runtime.json`;
  - contagem de `pending_offers.json`;
  - últimas linhas relevantes do log do processo.

## Fluxo de alerta operacional

1. **GitHub Actions**: monitorar falhas na página de Actions e na aba de runs do workflow `BOT LEOUOL - Consumer`.
2. **Notificação**: configurar integração de notificação (ex.: e-mail do GitHub, Slack, webhook, ou alerta via Telegram do time de operação) para qualquer run com status `failed` nesse workflow.
3. **Triagem**: ao receber alerta, abrir logs da run e revisar primeiro o step `diagnóstico de falha do consumer`.
4. **Ação corretiva**: corrigir causa raiz (dados, token, integração Telegram, indisponibilidade externa, etc.) e reexecutar via `workflow_dispatch`.

## Recomendação mínima de notificação

- Ativar notificação de falha de workflow para mantenedores no GitHub (Actions + e-mail).
- Se já existir canal de operações (Slack/Telegram), encaminhar eventos de falha para reduzir MTTR.
