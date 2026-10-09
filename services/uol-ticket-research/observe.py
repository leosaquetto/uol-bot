#!/usr/bin/env python3
"""Bounded public observations; no login, redemption, or notification delivery."""

import argparse
import concurrent.futures
import copy
import csv
import fcntl
import hashlib
import importlib
import io
import json
import os
from pathlib import Path
import re
import string
import sys
import tempfile
from datetime import datetime, timezone
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

ALPHABET = string.digits + string.ascii_uppercase + string.ascii_lowercase
MAX_CODES = 560
MAX_REQUESTS = 650
MAX_WORKERS = 2
DEFAULT_DIR = Path.home() / ".local/share/uol-ticket-research"
DEFAULT_CONFIG = {
    "prefixes": ["p" + letter for letter in "MNOPQRST"],
    "known_codes": ["pJq"],
    "metadata": {},
    "priority_target": "Pussycat Dolls 30/10/2026",
}
LOCAL_ZONE = ZoneInfo("America/Sao_Paulo")


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def local_day(timestamp):
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(LOCAL_ZONE).date().isoformat()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def new_state():
    return {"schema_version": 1, "config": copy.deepcopy(DEFAULT_CONFIG), "codes": {}, "runs": {}, "health": {}}


def load_state(data_dir):
    path = Path(data_dir) / "state.json"
    state = json.loads(path.read_text()) if path.exists() else new_state()
    state.setdefault("schema_version", 1)
    config = state.setdefault("config", {})
    for key, value in DEFAULT_CONFIG.items():
        config.setdefault(key, copy.deepcopy(value))
    for key in ("codes", "runs", "health"):
        state.setdefault(key, {})
    for prefix in config["prefixes"]:
        if not isinstance(prefix, str) or not re.fullmatch(r"[A-Za-z0-9]{2}", prefix):
            raise ValueError("Invalid prefix: expected two case-sensitive ASCII alphanumeric characters")
    for code in list(config["known_codes"]) + list(state["codes"]):
        if not isinstance(code, str) or not re.fullmatch(r"[A-Za-z0-9]{3}", code):
            raise ValueError("Invalid code: expected three case-sensitive ASCII alphanumeric characters")
    return state


def ticket_offer(offer, config):
    code = offer.get("code", "")
    url = urlsplit(str(offer.get("url", "")))
    return url.scheme == "https" and url.netloc == "clube.uol.com.br" and bool(re.fullmatch(r"/campanhasdeingresso/" + re.escape(str(code)) + r"-[^/]+/?", url.path))


def make_plan(state, catalogs=None):
    config = state["config"]
    offers = [offer for offer in (catalogs or {}).get("offers", []) if ticket_offer(offer, config)]
    selected = []
    review = []

    def add(code):
        if not re.fullmatch(r"[A-Za-z0-9]{3}", str(code)):
            review.append("invalid_catalog_code")
        elif code not in selected:
            if len(selected) < MAX_CODES:
                selected.append(code)
            else:
                review.append("code_limit")

    for code in config["known_codes"]:
        add(code)
    for code, record in state["codes"].items():
        if record.get("first_seen") or record.get("first_listed"):
            add(code)
    for offer in offers:
        add(offer.get("code"))
    latest_prefixes = {}
    for offer in (catalogs or {}).get("offers", []):
        code = offer.get("code", "")
        url = urlsplit(str(offer.get("url", "")))
        if ("https://clube.uol.com.br/?order=new" in offer.get("catalogs", [])
                and url.scheme == "https" and url.netloc == "clube.uol.com.br"
                and re.fullmatch(r"[A-Za-z0-9]{3}", str(code))):
            latest_prefixes.setdefault(code[:2], set()).add(code)
    new_prefixes = sorted(prefix for prefix, codes in latest_prefixes.items() if len(codes) >= 2 and prefix not in config["prefixes"])
    accepted_new_prefixes = []
    for prefix in list(dict.fromkeys(config["prefixes"])) + new_prefixes:
        candidates = [prefix + letter for letter in ALPHABET if prefix + letter not in selected]
        if len(selected) + len(candidates) <= MAX_CODES:
            selected.extend(candidates)
            if prefix in new_prefixes:
                accepted_new_prefixes.append(prefix)
        else:
            review.append("prefix_requires_review:" + prefix)
    return {
        "codes": selected, "code_count": len(selected), "max_codes": MAX_CODES,
        "max_requests": MAX_REQUESTS, "workers": MAX_WORKERS,
        "prefixes": config["prefixes"], "new_prefixes": new_prefixes,
        "accepted_new_prefixes": accepted_new_prefixes,
        "scope_review_required": bool(review), "scope_reasons": sorted(set(review)),
    }


def transition(record, changes, code, kind, before, after, **extra):
    item = {"code": code, "kind": kind, "previous_observed_at": before, "observed_at": after, **extra}
    record.setdefault("transitions", []).append(item)
    changes.append(item)


def clean_text(value):
    return re.sub(r"\s+([.,;:!?])", r"\1", " ".join(str(value).split()))


def detail_values(observation):
    return {"title": clean_text(observation.get("title", "")), "description": clean_text(observation.get("description", "")), "validity": [clean_text(value) for value in observation.get("validity", [])]}


def validity_values(values):
    dates = [item for value in values for item in re.findall(r"\d{2}/\d{2}/\d{4}(?:\s+\d{2}:\d{2})?", clean_text(value))]
    return dates or [clean_text(value) for value in values]


def detail_fingerprint(details):
    return digest({**details, "validity": validity_values(details["validity"])})


def apply_observations(state, observations, catalogs, catalog_urls):
    changes = []
    listing_time = catalogs["checkedAt"]
    signature = digest(sorted(set(catalog_urls)))
    complete = bool(catalogs.get("complete"))
    listed = {offer["code"]: offer for offer in catalogs.get("offers", []) if re.fullmatch(r"[A-Za-z0-9]{3}", str(offer.get("code", ""))) and ticket_offer(offer, state["config"])}
    for observation in observations:
        code, status = observation["code"], observation["status"]
        observed_at = observation["observedAt"]
        record = state["codes"].setdefault(code, {})
        prior_status = record.get("last_conclusive_page_status")
        prior_at = record.get("last_conclusive_page_at")
        record.update(last_checked=observed_at, last_status=status, last_reason=observation.get("reason"))
        if status == "found":
            if not record.get("first_seen"):
                record["first_seen"] = observed_at
                transition(record, changes, code, "first_seen", prior_at, observed_at)
            elif prior_status == "absent":
                transition(record, changes, code, "page_reappeared", prior_at, observed_at)
            details = detail_values(observation)
            detail_hash = detail_fingerprint(details)
            if record.get("detail_hash") and record["detail_hash"] != detail_hash:
                detail_at = record.get("last_seen", prior_at)
                transition(record, changes, code, "detail_changed", detail_at, observed_at)
                if validity_values(record.get("validity", [])) != validity_values(details["validity"]):
                    transition(record, changes, code, "validity_changed", detail_at, observed_at, previous_validity=record.get("validity", []), validity=details["validity"])
            record.update(details)
            record.update(last_seen=observed_at, detail_hash=detail_hash, url=observation.get("link", record.get("url", "")))
        elif status == "absent" and prior_status == "found":
            transition(record, changes, code, "page_gone", prior_at, observed_at)
        if status in ("found", "absent"):
            record.update(last_conclusive_page_status=status, last_conclusive_page_at=observed_at)

        prior_listing = record.get("listing_state", "unknown")
        prior_listing_at = record.get("listing_observed_at")
        previous_signature = record.get("listing_coverage_signature")
        record.setdefault("listing_state", "unknown")
        record["last_listing_check"] = {"observed_at": listing_time, "complete": complete, "signature": signature}
        if code in listed:
            if prior_listing != "listed":
                transition(record, changes, code, "listed", prior_listing_at, listing_time)
            record.setdefault("first_listed", listing_time)
            record.update(last_listed=listing_time, listing_state="listed", listing_coverage_signature=signature, listing_observed_at=listing_time, listing_catalogs=listed[code].get("catalogs", []))
        elif complete and (previous_signature in (None, signature)):
            if prior_listing == "listed":
                transition(record, changes, code, "unlisted", prior_listing_at, listing_time)
            record.update(listing_state="not_listed", listing_coverage_signature=signature, listing_observed_at=listing_time)
    return changes


def build_summary(state, observations, catalogs, plan, changes, started_at, requests, blocked_reason):
    problems = []
    if not catalogs.get("complete"):
        problems.append("catalog_incomplete")
    unknown = [item for item in observations if item["status"] == "unknown"]
    problems.extend("page_unknown:" + str(item.get("reason", "unknown")) for item in unknown)
    if plan["scope_review_required"]:
        problems.append("scope_review_required")
    if blocked_reason:
        problems.append("request_blocked:" + str(blocked_reason))
    problems = sorted(set(problems))
    health = state["health"]
    previous = health.get("problems", [])
    health_change = "failure" if problems and previous != problems else "recovery" if previous and not problems else None
    health.update(problems=problems, observed_at=started_at)
    useful_changes = [change for change in changes if change["kind"] in {"first_seen", "page_reappeared", "page_gone", "listed", "unlisted", "detail_changed", "validity_changed"}]
    return {
        "date": local_day(started_at), "observed_at": started_at,
        "status": "partial" if problems else "recorded", "requests": requests,
        "codes_checked": len(observations), "found": sum(item["status"] == "found" for item in observations),
        "absent": sum(item["status"] == "absent" for item in observations), "unknown": len(unknown),
        "catalog_complete": bool(catalogs.get("complete")),
        "scope_review_required": plan["scope_review_required"], "scope_reasons": plan["scope_reasons"],
        "changes": useful_changes, "health_change": health_change, "problems": problems,
        "should_notify": bool(useful_changes or health_change),
        "priority_target": state["config"]["priority_target"],
        "limitations": "Observações diárias (~24 h) podem perder eventos transitórios. Primeira aparição é a primeira observação, não cadastro/publicação. Página ausente não prova estoque ou esgotamento. Amostra limitada aos códigos e listas monitorados.",
    }


def atomic_write(path, content):
    path = Path(path)
    descriptor, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def immutable_write(path, content):
    descriptor, temporary = tempfile.mkstemp(prefix=".snapshot.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    finally:
        os.unlink(temporary)


def json_text(value):
    return json.dumps(value, ensure_ascii=False, indent=2) + "\n"


def listing_label(record):
    if not record.get("first_listed") and record.get("listing_state") != "listed":
        return "não observado nas listas monitoradas"
    return {"listed": "listado na última observação conclusiva", "not_listed": "não listado na última cobertura completa", "unknown": "inconclusivo"}.get(record.get("listing_state"), "inconclusivo")


def export_reports(data_dir, state, summary):
    columns = ["codigo", "titulo", "evento", "data", "validade", "primeira_aparicao", "ultima_aparicao", "listagem", "primeira_listagem", "ultima_listagem", "intervalos", "ultimo_status", "url", "tipo_registro", "fonte_referencia"]
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    metadata_by_code = state["config"].get("metadata", {})
    codes = set(state["codes"]) | {code for code, metadata in metadata_by_code.items() if metadata.get("referenceSource")}

    def event_date(code):
        value = metadata_by_code.get(code, {}).get("sortDate", "")
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value)):
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                pass
        return None

    dates = {code: event_date(code) for code in codes}
    ordered_codes = sorted(codes, key=lambda code: (-(dates[code].toordinal() if dates[code] else 0), code))
    report = ["# Histórico público de ingressos UOL", "", f"Rodada: {summary['date']} — {summary['status']}.", f"GETs: {summary['requests']}; códigos: {summary['codes_checked']}; páginas encontradas: {summary['found']}; inconclusivas: {summary['unknown']}.", "", summary["limitations"], "", f"Alvo prioritário: {summary['priority_target']}.", "", "Ordem: data do evento, do futuro para o passado; datas desconhecidas ao final de cada seção.", "", "## Páginas já observadas publicamente", ""]
    references = []
    for code in ordered_codes:
        record = state["codes"].get(code, {})
        metadata = metadata_by_code.get(code, {})
        event_name = metadata.get("eventName") or metadata.get("event", "")
        displayed_date = metadata.get("eventDate") or metadata.get("date") or (dates[code].isoformat() if dates[code] else "")
        reference = metadata.get("referenceSource", "")
        record_type = "observação pública" if record.get("first_seen") else "referência histórica" if reference else "sem observação pública confirmada"
        row = dict(zip(columns, [code, record.get("title", ""), event_name, displayed_date, " | ".join(record.get("validity", [])), record.get("first_seen", ""), record.get("last_seen", ""), listing_label(record), record.get("first_listed", ""), record.get("last_listed", ""), json.dumps(record.get("transitions", []), ensure_ascii=False), record.get("last_status", ""), record.get("url", ""), record_type, reference]))
        # Public titles can still begin with spreadsheet formula characters.
        writer.writerow({key: "'" + value if isinstance(value, str) and value.startswith(("=", "+", "-", "@")) else value for key, value in row.items()})
        chronology = "data do evento não classificada"
        if dates[code]:
            sort_date = dates[code].isoformat()
            chronology = "evento passado" if sort_date < summary["date"] else "evento hoje" if sort_date == summary["date"] else "evento futuro"
        title = str(event_name or record.get("title", "") or "Evento não identificado").replace("\n", " ")
        event_label = title + (" — " + str(displayed_date).replace("\n", " ") if displayed_date else "")
        reference_label = str(reference).replace("\n", " ")
        if record.get("first_seen"):
            source = f"; referência histórica: {reference_label}" if reference else ""
            report.append(f"- `{code}` — {event_label}; {chronology}; observação pública: primeira observação {record['first_seen']}; {listing_label(record)}; página: {record.get('last_status', 'unknown')}{source}.")
        elif reference:
            references.append(f"- `{code}` — {event_label}; {chronology}; referência histórica: {reference_label}; sem observação pública confirmada pelo coletor.")
    if references:
        report.extend(["", "## Referências históricas sem observação pública confirmada", "", *references])
    report.extend(["", "## Mudanças observadas", ""])
    report.extend(f"- `{item['code']}`: {item['kind']}; entre {item['previous_observed_at'] or 'início desconhecido'} e {item['observed_at']}." for item in summary["changes"])
    if summary["problems"]:
        report.extend(["", "Pendências: " + ", ".join(summary["problems"]) + "."])
    atomic_write(Path(data_dir) / "observations.csv", buffer.getvalue())
    atomic_write(Path(data_dir) / "report.md", "\n".join(report) + "\n")


def recover_snapshot(data_dir, state, day):
    """Finish local persistence after a crash, never repeat a recorded day's GETs."""
    snapshots = Path(data_dir) / "snapshots"
    if not snapshots.exists():
        return None
    for path in sorted(snapshots.glob("*.json"), reverse=True):
        snapshot = json.loads(path.read_text())
        if snapshot.get("date") != day:
            continue
        recovered = snapshot.get("state_after")
        if recovered is None:
            raise ValueError("Snapshot exists without recoverable state; review required before more requests")
        atomic_write(Path(data_dir) / "state.json", json_text(recovered))
        atomic_write(Path(data_dir) / "summary.json", json_text(snapshot["summary"]))
        export_reports(data_dir, recovered, snapshot["summary"])
        return {"status": "already_recorded", "date": day, "snapshot": str(path), "recorded_status": snapshot["summary"]["status"], "should_notify": False}
    return None


def run(data_dir=DEFAULT_DIR, backend=None, now=None):
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    with (data_dir / ".lock").open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Another observation is running; no history files changed") from error
        started_at = now or utc_now()
        day = local_day(started_at)
        state = load_state(data_dir)
        if day in state["runs"]:
            return {"status": "already_recorded", "date": day, "recorded_status": state["runs"][day]["status"], "should_notify": False}
        recovered = recover_snapshot(data_dir, state, day)
        if recovered:
            return recovered
        backend = backend or importlib.import_module("probe")
        backend.configure(max_requests=MAX_REQUESTS, deadline_seconds=480)
        try:
            catalogs = backend.fetch_catalogs()
        except Exception as error:
            catalogs = {"checkedAt": started_at, "pages": [], "offers": [], "complete": False, "error": type(error).__name__}
        catalogs.setdefault("checkedAt", started_at)
        plan = make_plan(state, catalogs)

        def check(code):
            try:
                result = backend.probe_code(code)
                if result.get("status") not in ("found", "absent", "unknown"):
                    raise ValueError("invalid_status")
                result["code"] = code
                result.setdefault("observedAt", utc_now())
                return result
            except Exception as error:
                return {"code": code, "status": "unknown", "reason": type(error).__name__, "observedAt": utc_now()}

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            observations = list(executor.map(check, plan["codes"]))
        changes = apply_observations(state, observations, catalogs, backend.CATALOG_URLS)
        for prefix in plan["accepted_new_prefixes"]:
            if prefix not in state["config"]["prefixes"]:
                state["config"]["prefixes"].append(prefix)
        summary = build_summary(state, observations, catalogs, plan, changes, started_at, backend.request_count(), backend.blocked_reason())
        snapshot_name = datetime.fromisoformat(started_at.replace("Z", "+00:00")).astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".json"
        state["runs"][day] = {"status": summary["status"], "observed_at": started_at, "snapshot": "snapshots/" + snapshot_name}
        snapshot = {"schema_version": 1, "date": day, "started_at": started_at, "plan": plan, "catalogs": catalogs, "observations": observations, "changes": changes, "summary": summary, "state_after": state}
        snapshots = data_dir / "snapshots"
        snapshots.mkdir(exist_ok=True)
        # Atomic exclusive publication: an existing snapshot is never overwritten.
        immutable_write(snapshots / snapshot_name, json_text(snapshot))
        atomic_write(data_dir / "state.json", json_text(state))
        atomic_write(data_dir / "summary.json", json_text(summary))
        export_reports(data_dir, state, summary)
        return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DIR)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--plan", action="store_true", help="Show bounded scope without network or writes")
    action.add_argument("--run", action="store_true", help="Record today's public GET observations once")
    args = parser.parse_args()
    try:
        result = make_plan(load_state(args.data_dir)) if args.plan else run(args.data_dir)
    except (OSError, ValueError, RuntimeError) as error:
        print(json.dumps({"status": "error", "reason": str(error)}, ensure_ascii=False), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
