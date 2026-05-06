# Rotina diária do guard (manual/atalho)

## Objetivo
Executar **1x por dia** uma validação resumida de:
1. Última decisão do guard (`run/skip`) e motivo.
2. Idade do `pipeline_audit.jsonl` no momento da decisão.
3. Se houve execução de fallback nas últimas 24h.
4. Se lock ficou preso (lock mais antigo que timeout).
5. Alertas de inconsistência no ledger/audit.

## Comando
```bash
python3 guard_daily_check.py
```

## Saída
- O script imprime uma linha JSON no terminal.
- Também adiciona a mesma linha em `run/guard_daily_check.log`.
- Quando detectar anomalias no audit, grava alertas em `run/uol_ios_fallback_alerts.jsonl`.

Campos principais:
- `ts_utc`: timestamp UTC da checagem.
- `status`: `OK` ou `ALERTA`.
- `decision`: última decisão (`run_fallback`, `skip_fallback`, etc.).
- `reason`: motivo da decisão.
- `pipeline_audit_age_min_at_decision`: idade em minutos no instante da decisão.
- `fallback_last_24h`: `true/false`.
- `lock_stuck`: `true/false`.
- `issues`: lista resumida de alertas.
- `audit_alert_count`: quantidade de anomalias detectadas no audit.

Anomalias monitoradas:
- `duplicate_offer_key_short_window`: mesma `offer_key` processada novamente em até N minutos (default: 60).
- `inconsistent_processed_without_valid_timestamp`: entrada com status de processada sem timestamp válido.
- `decision_skip_with_fallback_execution`: decisão de `skip` divergente com execução real de fallback.

## Atalho dedicado (Shortcuts iOS/macOS)
Criar um atalho com 1 ação de shell/SSH que execute:
```bash
cd /workspace/uol-bot && python3 guard_daily_check.py
```

> Se quiser ajustar caminhos/timeout:
```bash
python3 guard_daily_check.py \
  --decision-file run/uol_ios_fallback_decision.json \
  --audit-file run/uol_ios_fallback_audit.jsonl \
  --pipeline-audit-file pipeline_audit.jsonl \
  --lock-file run/uol_ios_fallback_lock.json \
  --lock-timeout-sec 1800 \
  --duplicate-window-min 60 \
  --alerts-file run/uol_ios_fallback_alerts.jsonl \
  --log-file run/guard_daily_check.log
```

## Rotina mensal (3 cenários)

Executar **1x por mês** os cenários abaixo e validar, em cada um:
- decisão do guard correta;
- fallback acionado somente quando devido;
- zero duplicidade no ledger.

### Cenários
1. **Mac online + pipeline fresco**
2. **Mac online + pipeline stale**
3. **Mac offline**

### Checklist simples (UTC)
> Preencha a data em UTC no formato `YYYY-MM-DD`.

- [ ] `YYYY-MM-DD` (UTC) — **Cenário 1: Mac online + pipeline fresco**
  - [ ] decisão do guard correta
  - [ ] fallback acionado somente quando devido
  - [ ] zero duplicidade no ledger
- [ ] `YYYY-MM-DD` (UTC) — **Cenário 2: Mac online + pipeline stale**
  - [ ] decisão do guard correta
  - [ ] fallback acionado somente quando devido
  - [ ] zero duplicidade no ledger
- [ ] `YYYY-MM-DD` (UTC) — **Cenário 3: Mac offline**
  - [ ] decisão do guard correta
  - [ ] fallback acionado somente quando devido
  - [ ] zero duplicidade no ledger
