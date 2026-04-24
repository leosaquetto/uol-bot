import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Dict

DEFAULT_STATUS_RUNTIME: Dict[str, Dict[str, Any]] = {
    "scriptable": {
        "last_started_at": "",
        "last_finished_at": "",
        "last_success_at": "",
        "status": "",
        "summary": "",
        "offers_seen": 0,
        "new_offers": 0,
        "pending_count": 0,
        "last_error": "",
    },
    "scraper": {
        "last_started_at": "",
        "last_finished_at": "",
        "last_success_at": "",
        "status": "",
        "summary": "",
        "offers_seen": 0,
        "new_offers": 0,
        "pending_count": 0,
        "last_error": "",
    },
    "consumer": {
        "last_started_at": "",
        "last_finished_at": "",
        "last_success_at": "",
        "status": "",
        "summary": "",
        "processed": 0,
        "sent": 0,
        "failed": 0,
        "pending_count": 0,
        "last_error": "",
    },
    "global": {
        "last_offer_title": "",
        "last_offer_at": "",
        "last_offer_id": "",
    },
}

CRITICAL_COMPONENT_KEYS: Dict[str, set[str]] = {
    "scriptable": {"status", "summary", "last_finished_at", "pending_count", "last_error"},
    "scraper": {"status", "summary", "last_finished_at", "pending_count", "last_error"},
    "consumer": {"status", "summary", "last_finished_at", "pending_count", "last_error"},
}


def _safe_load(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _normalize_status_runtime(data: Dict[str, Any]) -> Dict[str, Any]:
    out = data if isinstance(data, dict) else {}
    for component, template in DEFAULT_STATUS_RUNTIME.items():
        current = out.get(component)
        if not isinstance(current, dict):
            out[component] = deepcopy(template)
            continue
        normalized = deepcopy(template)
        normalized.update(current)
        out[component] = normalized
    return out


def load_status_runtime_file(path: str) -> Dict[str, Any]:
    return _normalize_status_runtime(_safe_load(Path(path)))


def merge_component_status_file(
    path: str,
    component: str,
    component_patch: Dict[str, Any],
    logger: Callable[[str], None] = print,
) -> Dict[str, Any]:
    if component not in DEFAULT_STATUS_RUNTIME:
        raise ValueError(f"componente inválido para status runtime: {component}")

    runtime_path = Path(path)
    status = load_status_runtime_file(path)

    component_state = status.get(component)
    if not isinstance(component_state, dict):
        component_state = deepcopy(DEFAULT_STATUS_RUNTIME[component])

    merged_component = deepcopy(component_state)
    merged_component.update(component_patch or {})
    status[component] = merged_component

    runtime_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

    reloaded_raw = _safe_load(runtime_path)
    reloaded = _normalize_status_runtime(reloaded_raw)
    reloaded_component = (
        reloaded_raw.get(component, {})
        if isinstance(reloaded_raw, dict) and isinstance(reloaded_raw.get(component), dict)
        else {}
    )
    missing = [
        key
        for key in sorted(CRITICAL_COMPONENT_KEYS.get(component, set()))
        if key not in reloaded_component
    ]
    if missing:
        logger(
            f"⚠️ warning status_runtime.json: chaves críticas ausentes em '{component}': {', '.join(missing)}"
        )

    return reloaded
