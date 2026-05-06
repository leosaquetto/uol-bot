#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

UTC = timezone.utc


def now_utc() -> datetime:
    return datetime.now(UTC)


def parse_iso(value: str) -> Optional[datetime]:
    try:
        v = value.strip()
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


def read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(obj)
        except Exception:
            continue
    return rows


def evaluate(args: argparse.Namespace) -> Tuple[str, Dict[str, Any]]:
    decision_path = Path(args.decision_file)
    audit_path = Path(args.audit_file)
    pipeline_audit_path = Path(args.pipeline_audit_file)
    lock_path = Path(args.lock_file)

    decision = read_json(decision_path) or {}
    audit_rows = read_jsonl(audit_path)

    decision_value = str(decision.get("decision") or "unknown")
    reason = str(decision.get("reason") or "unknown")
    decided_at = parse_iso(str(decision.get("recorded_at_utc") or ""))

    pipeline_age_min = None
    if decided_at and pipeline_audit_path.exists():
        pipeline_mtime = datetime.fromtimestamp(pipeline_audit_path.stat().st_mtime, tz=UTC)
        pipeline_age_min = round((decided_at - pipeline_mtime).total_seconds() / 60, 2)

    cutoff = now_utc() - timedelta(hours=24)
    fallback_last_24h = False
    for row in audit_rows:
        ts = parse_iso(str(row.get("ts_utc") or row.get("recorded_at_utc") or ""))
        if not ts or ts < cutoff:
            continue
        event = str(row.get("event") or "")
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if payload.get("run_fallback") is True:
            fallback_last_24h = True
            break
        if "fallback" in event and row.get("run_fallback") is True:
            fallback_last_24h = True
            break

    lock_stuck = False
    lock_age_sec = None
    if lock_path.exists():
        lock_data = read_json(lock_path) or {}
        started_ts = float(lock_data.get("started_ts") or 0)
        if started_ts > 0:
            age = now_utc().timestamp() * 1000 - started_ts
            lock_age_sec = int(age / 1000)
            lock_stuck = lock_age_sec > int(args.lock_timeout_sec)

    issues: List[str] = []
    if decision_value == "unknown":
        issues.append("decision_missing")
    if pipeline_age_min is None:
        issues.append("pipeline_age_unavailable")
    if lock_stuck:
        issues.append("lock_stuck")

    status = "OK" if not issues else "ALERTA"

    summary = {
        "ts_utc": now_utc().isoformat().replace("+00:00", "Z"),
        "status": status,
        "decision": decision_value,
        "reason": reason,
        "pipeline_audit_age_min_at_decision": pipeline_age_min,
        "fallback_last_24h": fallback_last_24h,
        "lock_stuck": lock_stuck,
        "lock_age_sec": lock_age_sec,
        "issues": issues,
    }
    return status, summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Validação diária do guard/fallback")
    parser.add_argument("--decision-file", default="run/uol_ios_fallback_decision.json")
    parser.add_argument("--audit-file", default="run/uol_ios_fallback_audit.jsonl")
    parser.add_argument("--pipeline-audit-file", default="pipeline_audit.jsonl")
    parser.add_argument("--lock-file", default="run/uol_ios_fallback_lock.json")
    parser.add_argument("--lock-timeout-sec", type=int, default=1800)
    parser.add_argument("--log-file", default="run/guard_daily_check.log")
    args = parser.parse_args()

    _, summary = evaluate(args)

    line = json.dumps(summary, ensure_ascii=False)
    print(line)

    log_path = Path(args.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
