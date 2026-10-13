#!/usr/bin/env python3
"""Offline patterns from public research state; no requests or state writes."""

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import re
import sys


CODE_ALPHABET_HYPOTHESIS = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
TRANSITION_KINDS = {"first_seen", "page_reappeared", "page_gone", "listed", "unlisted", "validity_changed"}
DEFAULT_DIR = Path.home() / ".local/share/uol-ticket-research"


def _epoch(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.timestamp() if parsed.tzinfo else None
    except (TypeError, ValueError, OverflowError):
        return None


def _date(value):
    try:
        return date.fromisoformat(value) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value)) else None
    except (TypeError, ValueError):
        return None


def _interval(kind, lower, upper):
    before, after = _epoch(lower), _epoch(upper)
    confidence = "bounded" if before is not None and after is not None and before <= after else "baseline" if lower is None and after is not None else "unknown"
    return {"kind": kind, "after": lower, "by": upper, "confidence": confidence,
            "observation_gap_seconds": after - before if confidence == "bounded" else None}


def _first_window(record, kind, field):
    first = record.get(field)
    at = _epoch(first)
    transition = next((item for item in record.get("transitions", []) if item.get("kind") == kind
                       and (not first or item.get("observed_at") == first or at is not None and _epoch(item.get("observed_at")) == at)), None)
    if transition:
        return _interval(kind, transition.get("previous_observed_at"), transition.get("observed_at"))
    return _interval(kind, None, first)


def _episodes(record, entry_kinds, exit_kind, first_window):
    """Duration bounds concern observed presence, never stock or continuity."""
    events = [item for item in record.get("transitions", []) if item.get("kind") in entry_kinds | {exit_kind}]
    current = first_window if first_window["by"] and not any(item.get("kind") == first_window["kind"] and _epoch(item.get("observed_at")) == _epoch(first_window["by"]) for item in events) else None
    episodes = []
    for item in events:
        window = _interval(item["kind"], item.get("previous_observed_at"), item.get("observed_at"))
        if item["kind"] in entry_kinds:
            current = window
        elif current:
            entry_low, entry_high = _epoch(current["after"]), _epoch(current["by"])
            exit_low, exit_high = _epoch(window["after"]), _epoch(window["by"])
            usable = current["confidence"] != "unknown" and window["confidence"] != "unknown"
            lower = exit_low - entry_high if usable and entry_high is not None and exit_low is not None and exit_low >= entry_high else None
            upper = exit_high - entry_low if usable and entry_low is not None and exit_high is not None and exit_high >= entry_low else None
            episodes.append({"entry": current, "exit": window, "minimum_seconds": lower, "maximum_seconds": upper})
            current = None
    return episodes


def _validity_start(values):
    for value in values:
        match = re.search(r"\b(\d{2}/\d{2}/\d{4})(?:\s+(\d{2}:\d{2}))?\b", str(value))
        if match:
            text = match.group(0)
            try:
                datetime.strptime(text, "%d/%m/%Y %H:%M" if match.group(2) else "%d/%m/%Y")
            except ValueError:
                continue
            return {"value": text, "precision": "minute" if match.group(2) else "day", "timezone": "not_published"}
    return None


def _rank(code):
    if not re.fullmatch(r"[A-Za-z0-9]{3}", str(code)):
        return None
    value = 0
    for letter in code:
        value = value * len(CODE_ALPHABET_HYPOTHESIS) + CODE_ALPHABET_HYPOTHESIS.index(letter)
    return value


def _code_order(codes):
    ordered = sorted((code for code, record in codes.items() if record.get("first_seen") and _rank(code) is not None), key=_rank)
    gaps = []
    for prefix in sorted({code[:2] for code in ordered}, key=lambda prefix: _rank(prefix + CODE_ALPHABET_HYPOTHESIS[0])):
        suffixes = [CODE_ALPHABET_HYPOTHESIS.index(code[-1]) for code in ordered if code.startswith(prefix)]
        gaps.extend(prefix + CODE_ALPHABET_HYPOTHESIS[index] for index in range(min(suffixes), max(suffixes) + 1) if prefix + CODE_ALPHABET_HYPOTHESIS[index] not in ordered)
    return {"status": "hypothesis", "alphabet": CODE_ALPHABET_HYPOTHESIS, "case_sensitive": True,
            "basis": "Candidate ordering only; code order does not prove the allocation algorithm or creation/publication order.",
            "ordered_confirmed_codes": ordered,
            "missing_between_confirmed_codes": gaps,
            "gap_meaning": "No confirmed ticket page in this sample; may belong to another benefit. Not proof of an unused code, deleted page, or next allocation."}


def _show(code, record, metadata, as_of):
    event_day = _date(metadata.get("sortDate"))
    first_seen = _first_window(record, "first_seen", "first_seen")
    first_listed = _first_window(record, "listed", "first_listed")
    page_at = _epoch(record.get("first_seen"))
    last_seen = _epoch(record.get("last_seen"))
    timeline = [_interval(item["kind"], item.get("previous_observed_at"), item.get("observed_at"))
                for item in record.get("transitions", []) if item.get("kind") in TRANSITION_KINDS]
    listed_at = _epoch(record.get("first_listed"))
    relistings = sum(item["kind"] == "listed" and listed_at is not None and _epoch(item["by"]) is not None and _epoch(item["by"]) > listed_at for item in timeline)
    return {"code": code, "event_name": metadata["eventName"], "event_date_raw": metadata.get("eventDate", ""),
            "public_url": record.get("url") or metadata.get("url"),
            "sort_date": event_day.isoformat() if event_day else None, "sort_date_basis": metadata.get("sortBasis", "unknown"),
            "date_group": "unknown" if not as_of or not event_day else "past" if event_day < as_of else "current_or_future",
            "evidence_type": "public_observation" if record.get("first_seen") else "historical_reference",
            "last_page_status": record.get("last_status", "unknown"), "last_page_checked_at": record.get("last_checked"),
            "last_conclusive_page_status": record.get("last_conclusive_page_status", "unknown"),
            "last_conclusive_page_at": record.get("last_conclusive_page_at"),
            "listing_state": record.get("listing_state", "unknown"), "listing_observed_at": record.get("listing_observed_at"),
            "first_seen": first_seen, "first_seen_timestamp_basis": record.get("first_seen_timestamp_basis", "unknown"),
            "first_listed": first_listed, "last_seen": record.get("last_seen"), "last_listed": record.get("last_listed"),
            "publication_lead": "not_established",
            "observed_page_span_seconds": last_seen - page_at if page_at is not None and last_seen is not None and last_seen >= page_at else None,
            "page_span_meaning": "Time between observations, not continuous availability or campaign lifetime.",
            "validity_raw": record.get("validity", []), "validity_start": _validity_start(record.get("validity", [])),
            "timeline": timeline, "page_reappearances": sum(item["kind"] == "page_reappeared" for item in timeline),
            "relistings": relistings,
            "page_presence_episodes": _episodes(record, {"first_seen", "page_reappeared"}, "page_gone", first_seen),
            "listing_presence_episodes": _episodes(record, {"listed"}, "unlisted", first_listed)}


def _story_order(window, story, kind):
    publication = _epoch(story.get("instagramPublishedAt"))
    lower, upper = _epoch(window["after"]), _epoch(window["by"])
    if story.get("baseline") is not False or story.get("excludedReason") or window["confidence"] != "bounded" or publication is None:
        return "order_uncertain"
    if upper < publication:
        return kind + "_before_story"
    if publication <= lower:
        return "story_before_" + kind
    return "order_uncertain"


def _attach_stories(shows, stories, codes, metadata):
    if stories is None:
        return {"status": "unavailable", "reason": "no_local_history", "record_count": None, "observations": []}
    # Only sanitized history records from the offline exporter; never source IDs.
    records = stories.get("records", [])
    observations = []
    for story in records:
        code, url = story.get("code"), story.get("canonicalUrl")
        if not isinstance(code, str) or not re.fullmatch(r"p[A-Za-z0-9]{2,5}", code) or not isinstance(url, str) or not re.fullmatch(r"https://clube\.uol\.com\.br/campanhasdeingresso/" + re.escape(code) + r"-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*", url):
            continue
        record, info = codes.get(code, {}), metadata.get(code, {})
        known_url = record.get("url") or info.get("url")
        reference = {key: story.get(key) for key in ("canonicalUrl", "instagramPublishedAt", "instagramFirstSeenAt", "collectionTrigger", "baseline", "excludedReason", "pushCorrelation", "outboxStatus")}
        reference.update(code=code, event_name=info.get("eventName") or (record.get("title", "") if record.get("first_seen") else ""),
                         event_type=info.get("type", "unknown"), public_page_url_matches=known_url == url,
                         identity_basis="documented_metadata" if info.get("eventName") else "public_page_title" if record.get("first_seen") and record.get("title") else "unknown")
        if reference["collectionTrigger"] not in {"poll", "push", "safety", "unknown"}:
            reference["collectionTrigger"] = "unknown"
        if reference["pushCorrelation"] not in {"exact_story_id", "without_corresponding_push", "generic_profile_signal_uncertain", "awaiting_push"}:
            reference["pushCorrelation"] = "unknown"
        reference["targets"] = {target: {key: value.get(key) for key in ("status", "imageConfirmed")}
                                 for target, value in story.get("targets", {}).items() if target in {"main", "canal2", "discord", "beeper"} and isinstance(value, dict)}
        reference["page_story_order"] = _story_order(_first_window(record, "first_seen", "first_seen"), story, "page") if known_url == url else "order_uncertain"
        reference["catalog_story_order"] = _story_order(_first_window(record, "listed", "first_listed"), story, "catalog") if known_url == url else "order_uncertain"
        reference["order_basis"] = "Conclusive absent-to-found bounds compared to public Story publication; baseline/overlap remains uncertain. Source clocks were not independently verified."
        observations.append(reference)
    matched = 0
    for item in shows:
        item["instagram_references"] = [entry for entry in observations if entry["code"] == item["code"] and entry["canonicalUrl"] == item["public_url"]]
        matched += len(item["instagram_references"])
    return {"status": stories.get("status", "unknown"), "captured_at": stories.get("capturedAt"),
            "record_count": len(records) if stories.get("status") in {"available", "partial"} else None,
            "matched_show_references": matched, "invalid_record_count": stories.get("invalidRecordCount"),
            "truncated": stories.get("truncated"), "reason": stories.get("reason"), "observations": observations}


def build_patterns(state, summary, stories=None):
    """Return a read-only report anchored to summary.date, never today's clock."""
    codes = state.get("codes", {})
    metadata = state.get("config", {}).get("metadata", {})
    as_of = _date(summary.get("date"))
    confirmed = {code: record for code, record in codes.items() if record.get("first_seen")}
    ever_listed = lambda record: bool(record.get("first_listed") or record.get("listing_state") == "listed" or any(item.get("kind") in {"listed", "unlisted"} for item in record.get("transitions", [])))
    off_list = [{"code": code, "event_name": metadata.get(code, {}).get("eventName", ""),
                 "page_checked_at": record.get("last_checked"), "listing_observed_at": record.get("listing_observed_at"),
                 "listing_history": "listed_then_removed" if ever_listed(record) else "never_observed_listed"}
                for code, record in sorted(confirmed.items()) if record.get("last_status") == "found" and record.get("listing_state") == "not_listed"]
    shows = [_show(code, codes.get(code, {}), item, as_of) for code, item in metadata.items()
             if item.get("type") == "Show" and item.get("eventName") and (codes.get(code, {}).get("first_seen") or item.get("referenceSource") or item.get("historical"))]
    shows.sort(key=lambda item: (-(date.fromisoformat(item["sort_date"]).toordinal() if item["sort_date"] else 0), item["code"]))
    instagram_history = _attach_stories(shows, stories, codes, metadata)
    return {"schema_version": 1, "as_of_date": summary.get("date"), "sample_observed_at": summary.get("observed_at"),
            "sample_status": summary.get("status", "unknown"), "catalog_complete": summary.get("catalog_complete"),
            "counts": {"tracked_codes": len(codes), "confirmed_pages_ever": len(confirmed),
                       "latest_found": sum(record.get("last_status") == "found" for record in codes.values()),
                       "latest_absent": sum(record.get("last_status") == "absent" for record in codes.values()),
                       "latest_unknown": sum(record.get("last_status") not in {"found", "absent"} for record in codes.values()),
                       "confirmed_never_observed_listed": sum(not ever_listed(record) for record in confirmed.values()),
                       "confirmed_listed_then_removed": sum(ever_listed(record) and record.get("listing_state") == "not_listed" for record in confirmed.values()),
                       "latest_found_off_list": len(off_list), "identified_public_shows": sum(item["evidence_type"] == "public_observation" for item in shows),
                       "historical_show_references": sum(item["evidence_type"] == "historical_reference" for item in shows)},
            "current_or_future_shows": [item for item in shows if item["date_group"] == "current_or_future"],
            "past_shows": [item for item in shows if item["date_group"] == "past"],
            "unknown_date_shows": [item for item in shows if item["date_group"] == "unknown"],
            "found_off_list": off_list, "code_order": _code_order(codes), "instagram_history": instagram_history,
            "limitations": ["First seen/listed are collector observations, not publication or creation times.",
                            "Baseline observations and overlapping daily windows do not establish discovery lead.",
                            "Page/catalog presence does not prove stock, redemption eligibility, or exhaustion.",
                            "Not listed concerns monitored catalogs only; unknown preserves prior conclusive evidence.",
                            "Dates use documented metadata sortDate; raw event dates and inferred-year basis remain visible.",
                            "Only optional sanitized local Instagram history is compared; missing history does not prove no Stories.",
                            "Poll/push collection and per-target delivery/image evidence remain separate; historical references do not become public observations."]}


def render_markdown(report):
    """Compact human report; full timing bounds remain in JSON."""
    counts, order = report["counts"], report["code_order"]
    lines = ["# Padrões observados de ingressos UOL", "",
             f"Amostra: {report['as_of_date']} — {report['sample_status']}; horário: {report['sample_observed_at'] or 'desconhecido'}.",
             f"Códigos: {counts['tracked_codes']}; páginas já confirmadas: {counts['confirmed_pages_ever']}; última observação: {counts['latest_found']} encontradas, {counts['latest_absent']} ausentes, {counts['latest_unknown']} inconclusivas.",
             f"Confirmadas: {counts['confirmed_never_observed_listed']} nunca observadas nas listas; {counts['confirmed_listed_then_removed']} já listadas e retiradas; {counts['latest_found_off_list']} encontradas fora das listas na amostra.", "",
             "## Shows identificados", "", "Nome/tipo vêm dos metadados públicos. Data usa sortDate documentado; referência histórica permanece referência.", ""]
    for group, title in (("current_or_future_shows", "Hoje ou futuros"), ("past_shows", "Passados"), ("unknown_date_shows", "Data desconhecida")):
        lines.extend([f"### {title}", ""])
        if not report[group]:
            lines.append("Nenhum na amostra.")
        for item in report[group]:
            evidence = "observação pública" if item["evidence_type"] == "public_observation" else "referência histórica; sem observação pública confirmada"
            lines.append(f"- `{item['code']}` — {_text(item['event_name'])}; {_text(item['event_date_raw']) or 'data desconhecida'}; {evidence}; página {item['last_page_status']}; catálogo {item['listing_state']}.")
            if item["validity_raw"]:
                lines.append(f"  Validade publicada: {_text(' | '.join(item['validity_raw']))}")
            if item["evidence_type"] == "public_observation":
                first = item["first_seen"]
                lines.append(f"  Primeira observação: ({first['after'] or 'início desconhecido'}, {first['by']}] — {first['confidence']}; {_text(item['first_seen_timestamp_basis'])}.")
                listed = item["first_listed"]
                if listed["by"]:
                    lines.append(f"  Primeira listagem: ({listed['after'] or 'início desconhecido'}, {listed['by']}] — {listed['confidence']}; última página vista {item['last_seen']}; última listagem {item['last_listed']}.")
                lines.extend(f"  {event['kind']}: ({event['after'] or 'início desconhecido'}, {event['by']}] — {event['confidence']}." for event in item["timeline"] if event["kind"] != "first_seen")
            lines.append(f"  Base da ordem por data: {_text(item['sort_date_basis']).rstrip('.')}.")
            for story in item.get("instagram_references", []):
                lines.append(f"  Story: publicação {story['instagramPublishedAt']}; primeira coleta {story['instagramFirstSeenAt'] or 'desconhecida'}; coleta {story['collectionTrigger'] or 'unknown'}; página/Story {story['page_story_order']}; catálogo/Story {story['catalog_story_order']}; outbox {story['outboxStatus'] or 'unknown'}.")
        lines.append("")
    lines.extend(["## Páginas encontradas fora das listas", ""])
    lines.extend(f"- `{item['code']}` — {_text(item['event_name']) or 'evento não identificado'}; {item['listing_history']}; página {item['page_checked_at']}; catálogo {item['listing_observed_at']}." for item in report["found_off_list"])
    if not report["found_off_list"]:
        lines.append("Nenhuma confirmada na última observação.")
    history = report["instagram_history"]
    lines.extend(["", "## Stories capturados: histórico local", "", f"Cobertura: {history['status']}; ausência ou cobertura parcial não prova ausência de Stories."])
    for story in history["observations"]:
        lines.append(f"- `{story['code']}` — {_text(story['event_name']) or 'evento não identificado'}; publicação {story['instagramPublishedAt']}; primeira coleta {story['instagramFirstSeenAt'] or 'desconhecida'}; coleta {story['collectionTrigger']}; correlação push {story['pushCorrelation']}; baseline {story['baseline']}; exclusão {story['excludedReason'] or 'nenhuma registrada'}.")
        lines.append(f"  Página/Story: {story['page_story_order']}; catálogo/Story: {story['catalog_story_order']}; outbox: {story['outboxStatus'] or 'unknown'}.")
        targets = "; ".join(f"{target}: {value['status'] or 'unknown'}, imagem {value['imageConfirmed'] if value['imageConfirmed'] is not None else 'unknown'}" for target, value in story["targets"].items())
        lines.append("  Destinos: " + (targets or "sem confirmação disponível") + ".")
    lines.extend(["", "## Ordem dos códigos: hipótese", "", f"Alfabeto candidato: `{order['alphabet']}`; maiúsculas e minúsculas distintas.",
                  f"Lacunas entre códigos confirmados: {len(order['missing_between_confirmed_codes'])}. Podem pertencer a outros benefícios; ausência de confirmação não prova código livre nem próxima alocação.",
                  "Validade publicada não prova criação/publicação. Ordem candidata não confirma algoritmo gerador.", "", "## Limites", ""])
    lines.extend("- " + item for item in report["limitations"])
    return "\n".join(lines) + "\n"


def _text(value):
    return str(value).replace("\n", " ").replace("\r", " ")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DIR)
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    args = parser.parse_args()
    try:
        state = json.loads((args.data_dir / "state.json").read_text())
        summary = json.loads((args.data_dir / "summary.json").read_text())
        story_path = args.data_dir / "instagram-history.json"
        stories = json.loads(story_path.read_text()) if story_path.exists() else None
        report = build_patterns(state, summary, stories)
    except (OSError, ValueError, TypeError) as error:
        print(json.dumps({"status": "error", "reason": str(error)}, ensure_ascii=False), file=sys.stderr)
        return 1
    print(render_markdown(report) if args.format == "markdown" else json.dumps(report, ensure_ascii=False, indent=2), end="" if args.format == "markdown" else "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
