#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from legacy_safety import sanitize_audit_value

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
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
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


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(sanitize_audit_value(row), ensure_ascii=False) + "\n")


def write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    """Replace a report atomically so readers never observe partial JSON."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(sanitize_audit_value(payload), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def detect_audit_anomalies(
    audit_rows: List[Dict[str, Any]],
    decision: Dict[str, Any],
    duplicate_window_min: int,
    current_time: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []
    checked_at = current_time or now_utc()
    now_ts = checked_at.isoformat().replace("+00:00", "Z")

    processed_by_key: Dict[str, List[datetime]] = {}

    for idx, row in enumerate(audit_rows):
        offer_key = str(row.get("offer_key") or row.get("key") or "").strip()
        status = str(row.get("status") or row.get("event") or "").strip().lower()

        ts_str = str(
            row.get("processed_at_utc")
            or row.get("timestamp_utc")
            or row.get("at_utc")
            or row.get("ts_utc")
            or row.get("recorded_at_utc")
            or ""
        )
        row_ts = parse_iso(ts_str)

        is_processed = "process" in status or status in {"done", "success", "ok"}
        if is_processed and row_ts is None:
            alerts.append(
                {
                    "ts_utc": now_ts,
                    "alert_type": "inconsistent_processed_without_valid_timestamp",
                    "message": "Entrada processada sem timestamp válido.",
                    "row_index": idx,
                    "offer_key": offer_key,
                    "status": status,
                    "raw_timestamp": ts_str,
                }
            )

        if offer_key and row_ts and is_processed:
            processed_by_key.setdefault(offer_key, []).append(row_ts)

    window = timedelta(minutes=duplicate_window_min)
    for offer_key, timestamps in processed_by_key.items():
        timestamps.sort()
        for prev_ts, curr_ts in zip(timestamps, timestamps[1:]):
            delta = curr_ts - prev_ts
            if delta <= window:
                alerts.append(
                    {
                        "ts_utc": now_ts,
                        "alert_type": "duplicate_offer_key_short_window",
                        "message": "Mesma offer_key processada mais de uma vez em janela curta.",
                        "offer_key": offer_key,
                        "window_min": duplicate_window_min,
                        "first_processed_at_utc": prev_ts.isoformat().replace("+00:00", "Z"),
                        "second_processed_at_utc": curr_ts.isoformat().replace("+00:00", "Z"),
                        "delta_seconds": int(delta.total_seconds()),
                    }
                )
                break

    decision_value = str(decision.get("decision") or "").strip().lower()
    run_fallback = decision.get("run_fallback")
    decided_at = parse_iso(str(decision.get("recorded_at_utc") or ""))
    decision_indicates_skip = decision_value == "skip" or decision_value == "skip_fallback" or run_fallback is False
    if decision_indicates_skip:
        executed_fallback = False
        for row in reversed(audit_rows):
            row_ts = parse_iso(
                str(
                    row.get("processed_at_utc")
                    or row.get("timestamp_utc")
                    or row.get("at_utc")
                    or row.get("ts_utc")
                    or row.get("recorded_at_utc")
                    or ""
                )
            )
            if decided_at and (not row_ts or row_ts < decided_at):
                continue
            event = str(row.get("event") or "").lower()
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            if row.get("run_fallback") is True or payload.get("run_fallback") is True or "fallback_executed" in event:
                executed_fallback = True
                break
        if executed_fallback:
            alerts.append(
                {
                    "ts_utc": now_ts,
                    "alert_type": "decision_skip_with_fallback_execution",
                    "message": "Divergência: decisão de skip, porém fallback foi executado.",
                    "decision": decision_value,
                }
            )

    return alerts


def _number_or_none(value: Any) -> Optional[float]:
    try:
        number = float(value)
        return number if number >= 0 else None
    except (TypeError, ValueError):
        return None


def _derive_mac_online(decision: Dict[str, Any], reason: str) -> Optional[bool]:
    probe = decision.get("ssh_probe")
    if isinstance(probe, dict) and isinstance(probe.get("ok"), bool):
        return bool(probe["ok"])
    if reason == "mac_offline_or_inaccessible":
        return False
    if reason in {"missing_or_invalid_mtime", "pipeline_audit_recent", "pipeline_audit_stale"}:
        return True
    return None


def _derive_pipeline_fresh(decision: Dict[str, Any], reason: str, age_min: Optional[float]) -> Optional[bool]:
    if age_min is not None:
        stale_window = _number_or_none(decision.get("stale_window_min"))
        drift_margin = _number_or_none(decision.get("drift_margin_min")) or 0
        if stale_window is not None:
            return age_min <= stale_window + drift_margin
    if reason == "pipeline_audit_recent":
        return True
    if reason in {"pipeline_audit_stale", "missing_or_invalid_mtime", "mac_offline_or_inaccessible"}:
        return False
    return None


def evaluate(
    args: argparse.Namespace,
    current_time: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any], List[Dict[str, Any]]]:
    checked_at = current_time or now_utc()
    decision_path = Path(args.decision_file)
    audit_path = Path(args.audit_file)
    pipeline_audit_path = Path(args.pipeline_audit_file)
    lock_path = Path(args.lock_file)

    decision_data = read_json(decision_path)
    decision = decision_data or {}
    audit_rows = read_jsonl(audit_path)

    decision_value = str(decision.get("decision") or "unknown")
    reason = str(decision.get("reason") or "unknown")
    decided_at = parse_iso(str(decision.get("recorded_at_utc") or ""))

    pipeline_age_min = _number_or_none(decision.get("pipeline_audit_age_min"))
    pipeline_age_source = "decision" if pipeline_age_min is not None else None
    if pipeline_age_min is None and decided_at and pipeline_audit_path.exists():
        pipeline_mtime = datetime.fromtimestamp(pipeline_audit_path.stat().st_mtime, tz=UTC)
        calculated_age = (decided_at - pipeline_mtime).total_seconds() / 60
        if calculated_age >= 0:
            pipeline_age_min = round(calculated_age, 2)
            pipeline_age_source = "local_file_mtime"

    mac_online = _derive_mac_online(decision, reason)
    pipeline_fresh = _derive_pipeline_fresh(decision, reason, pipeline_age_min)

    cutoff = checked_at - timedelta(hours=24)
    fallback_last_24h = False
    for row in audit_rows:
        ts = parse_iso(
            str(
                row.get("timestamp_utc")
                or row.get("at_utc")
                or row.get("ts_utc")
                or row.get("recorded_at_utc")
                or ""
            )
        )
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

    lock_stale = False
    lock_invalid = False
    lock_age_sec = None
    if lock_path.exists():
        lock_data = read_json(lock_path)
        started_ts = _number_or_none(lock_data.get("started_ts")) if lock_data else None
        if started_ts and started_ts > 0:
            age = checked_at.timestamp() * 1000 - started_ts
            lock_age_sec = int(age / 1000)
            lock_stale = lock_age_sec > int(args.lock_timeout_sec)
        else:
            lock_invalid = True

    anomaly_alerts = detect_audit_anomalies(
        audit_rows,
        decision,
        args.duplicate_window_min,
        current_time=checked_at,
    )

    issues: List[str] = []
    if not decision_path.exists():
        issues.append("decision_missing")
    elif decision_data is None:
        issues.append("decision_invalid")
    elif decision_value == "unknown":
        issues.append("decision_unknown")
    if mac_online is False:
        issues.append("mac_offline")
    elif mac_online is None:
        issues.append("mac_status_unavailable")
    if pipeline_age_min is None:
        issues.append("pipeline_age_unavailable")
    if pipeline_fresh is False:
        issues.append("pipeline_stale")
    elif pipeline_fresh is None:
        issues.append("pipeline_freshness_unavailable")
    if lock_invalid:
        issues.append("lock_invalid")
    if lock_stale:
        issues.append("lock_stale")
    if anomaly_alerts:
        issues.append("audit_anomalies_detected")

    status = "OK" if not issues else "ALERTA"

    summary = {
        "report_schema_version": 2,
        "ts_utc": checked_at.isoformat().replace("+00:00", "Z"),
        "status": status,
        "decision": decision_value,
        "reason": reason,
        "decision_recorded_at_utc": decided_at.isoformat().replace("+00:00", "Z") if decided_at else None,
        "mac_online": mac_online,
        "pipeline_fresh": pipeline_fresh,
        "pipeline_audit_age_min_at_decision": pipeline_age_min,
        "pipeline_age_source": pipeline_age_source,
        "fallback_last_24h": fallback_last_24h,
        "lock_stale": lock_stale,
        "lock_valid": not lock_invalid,
        "lock_age_sec": lock_age_sec,
        "issues": issues,
        "audit_alert_count": len(anomaly_alerts),
    }
    return status, sanitize_audit_value(summary), sanitize_audit_value(anomaly_alerts)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validação diária do guard/fallback")
    parser.add_argument("--decision-file", default="run/uol_ios_fallback_decision.json")
    parser.add_argument("--audit-file", default="run/uol_ios_fallback_audit.jsonl")
    parser.add_argument("--pipeline-audit-file", default="pipeline_audit.jsonl")
    parser.add_argument("--lock-file", default="run/uol_ios_fallback_lock.json")
    parser.add_argument("--lock-timeout-sec", type=int, default=1800)
    parser.add_argument("--log-file", default="run/guard_daily_check.log")
    parser.add_argument("--alerts-file", default="run/uol_ios_fallback_alerts.jsonl")
    parser.add_argument("--report-file", default="run/uol_ios_fallback_report.json")
    parser.add_argument("--duplicate-window-min", type=int, default=60)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    status, summary, anomaly_alerts = evaluate(args)

    line = json.dumps(summary, ensure_ascii=False)
    print(line)

    log_path = Path(args.log_file)
    append_jsonl(log_path, summary)

    alerts_path = Path(args.alerts_file)
    for alert in anomaly_alerts:
        append_jsonl(alerts_path, alert)

    write_json_atomic(Path(args.report_file), summary)

    return 0 if status == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
