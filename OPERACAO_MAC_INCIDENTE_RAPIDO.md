# Operação rápida de incidente — Mac/Fallback

> Recuperação fria legada. Não rode em paralelo com o Worker ativo. O relatório
> representa a última decisão/probe persistida; não substitui uma checagem de
> rede em tempo real.

## 1) Mac saudável vs stale
Use o status consolidado:

```bash
python3 guard_daily_check.py; rc=$?
cat run/uol_ios_fallback_report.json
test "$rc" -eq 0
```

Leitura rápida no `run/uol_ios_fallback_report.json`:
- **Saudável**: `mac_online=true` e `pipeline_fresh=true`.
- **Stale**: `mac_online=true` e `pipeline_fresh=false`.
- **Crítico (Mac offline)**: `mac_online=false`.
- **Dados insuficientes**: valor `null`; o status será `ALERTA`.

O comando retorna `0` somente para `status=OK` e `1` para `status=ALERTA`. O
report é gravado atomicamente nos dois casos.

## 2) Logs de decisão / audit / ledger
Arquivos:
- Decisão: `run/uol_ios_fallback_decision.json`
- Audit fallback: `run/uol_ios_fallback_audit.jsonl`
- Audit pipeline (ledger operacional): `pipeline_audit.jsonl`

Comandos úteis:

```bash
cat run/uol_ios_fallback_decision.json

tail -n 50 run/uol_ios_fallback_audit.jsonl

tail -n 50 pipeline_audit.jsonl
```

Foco:
- `decision`: `run_fallback` vs `skip_fallback`.
- `reason`: motivo da decisão.
- `recorded_at_utc`: horário efetivo da decisão.
- Eventos duplicados/anômalos: verificar `audit_alert_count` no report.

## 3) Limpeza segura de lock órfão
1. Revalidar idade do lock:

```bash
python3 guard_daily_check.py --lock-timeout-sec 1800; rc=$?
cat run/uol_ios_fallback_report.json
test "$rc" -eq 0
```

2. Só quarentenar se `lock_stale=true`:

```bash
mv run/uol_ios_fallback_lock.json "run/uol_ios_fallback_lock.stale.$(date -u +%Y%m%dT%H%M%SZ).json"
```

O `mv` é recuperável; não apague o lock durante a investigação.

3. Rodar nova validação após remoção:

```bash
python3 guard_daily_check.py; rc=$?
cat run/uol_ios_fallback_report.json
test "$rc" -eq 0
```

## 4) Pausar fallback temporariamente (sem afetar ciclo do Mac)
Objetivo: manter Mac/ciclo normal e **forçar não execução** do fallback.

Ajuste a decisão manual para `skip_fallback`:

```bash
cat > run/uol_ios_fallback_decision.json <<'JSON'
{
  "decision": "skip_fallback",
  "run_fallback": false,
  "reason": "pause_temporaria_incidente",
  "recorded_at_utc": "YYYY-MM-DDTHH:MM:SSZ"
}
JSON
```

Depois, confirmar no audit que não houve execução de fallback enquanto a pausa está ativa.

## 5) Validar retorno à normalidade
Checklist objetivo:
- `mac_online=true`
- `pipeline_fresh=true`
- `lock_stale=false`
- `audit_alert_count=0`
- Sem divergência `decision_skip_with_fallback_execution`

Comando final:

```bash
python3 guard_daily_check.py; rc=$?
cat run/uol_ios_fallback_report.json
test "$rc" -eq 0
```
