import copy
import csv
import fcntl
import json
from pathlib import Path
import tempfile
import unittest

import observe

T1 = "2026-10-09T15:00:00Z"
T2 = "2026-10-10T15:00:00Z"
T3 = "2026-10-11T15:00:00Z"
URL = "https://clube.uol.com.br/?order=new"


def offer(code="pPS", category="ingressos", catalog=URL):
    return {"code": code, "url": f"https://clube.uol.com.br/campanhasdeingresso/{code}-evento", "title": "Evento", "category": category, "catalogs": [catalog]}


def catalog(at=T1, listed=True, complete=True):
    return {"checkedAt": at, "pages": [{"url": URL, "status": 200, "error": None, "unique_cards": 1}], "offers": [offer()] if listed else [], "complete": complete}


def observation(status="found", at=T1, **values):
    return {"code": "pPS", "status": status, "reason": "test", "observedAt": at, "title": "Show", "description": "Texto público do show.", "validity": ["De 01/08/2026 10:00 a 01/11/2026 13:00."], "link": "https://clube.uol.com.br/campanhasdeingresso/pPS-show", **values}


class FakeProbe:
    CATALOG_URLS = [URL]

    def __init__(self, at=T1, unknown=False):
        self.at = at
        self.unknown = unknown
        self.calls = []

    def configure(self, **limits):
        self.calls.append(("configure", limits))

    def fetch_catalogs(self):
        self.calls.append(("catalog",))
        return catalog(self.at, complete=not self.unknown)

    def probe_code(self, code):
        self.calls.append(("probe", code))
        return observation("unknown" if self.unknown else "found", self.at, code=code)

    def request_count(self):
        return len(self.calls) - 1

    def blocked_reason(self):
        return ""


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.state = observe.new_state()

    def apply(self, obs, cat):
        return observe.apply_observations(self.state, [obs], cat, [URL])

    def test_unknown_preserves_presence_and_listing(self):
        self.apply(observation(), catalog())
        original = copy.deepcopy(self.state["codes"]["pPS"])
        changes = self.apply(observation("unknown", T2), catalog(T2, False, False))
        record = self.state["codes"]["pPS"]
        self.assertEqual(changes, [])
        self.assertEqual(record["last_status"], "unknown")
        for key in ("first_seen", "last_seen", "last_conclusive_page_status", "last_conclusive_page_at", "listing_state", "last_listed", "listing_coverage_signature", "detail_hash"):
            self.assertEqual(record[key], original[key])

    def test_partial_catalog_does_not_unlist_and_complete_does(self):
        self.apply(observation(), catalog())
        self.apply(observation(at=T2), catalog(T2, False, False))
        changes = self.apply(observation(at=T3), catalog(T3, False, True))
        event = next(change for change in changes if change["kind"] == "unlisted")
        self.assertEqual(event["previous_observed_at"], T1)
        self.assertEqual(event["observed_at"], T3)
        self.assertEqual(self.state["codes"]["pPS"]["listing_state"], "not_listed")

    def test_changed_catalog_coverage_does_not_unlist(self):
        self.apply(observation(), catalog())
        changes = observe.apply_observations(self.state, [observation(at=T2)], catalog(T2, False), [URL, "https://clube.uol.com.br/?categoria=other"])
        self.assertFalse(any(change["kind"] == "unlisted" for change in changes))
        self.assertEqual(self.state["codes"]["pPS"]["listing_state"], "listed")

    def test_page_transitions_use_last_conclusive_status(self):
        self.apply(observation(), catalog())
        self.apply(observation("unknown", T2), catalog(T2, False, False))
        changes = self.apply(observation("absent", T3), catalog(T3, False, False))
        self.assertEqual([change["kind"] for change in changes], ["page_gone"])
        self.assertEqual(changes[0]["previous_observed_at"], T1)
        self.apply(observation("unknown", "2026-10-12T15:00:00Z"), catalog(T3, False, False))
        self.assertEqual(self.apply(observation("absent", "2026-10-13T15:00:00Z"), catalog(T3, False, False)), [])

    def test_first_seen_is_real_observation_not_benefit_start(self):
        self.apply(observation(), catalog())
        self.assertEqual(self.state["codes"]["pPS"]["first_seen"], T1)
        self.assertNotIn("2026-08", self.state["codes"]["pPS"]["first_seen"])

    def test_metadata_hash_ignores_timestamp_and_html_spacing(self):
        self.apply(observation(), catalog())
        changes = self.apply(observation(at=T2, validity=["De 01/08/2026 10:00 a 01/11/2026 13:00 ."]), catalog(T2))
        self.assertEqual(changes, [])
        changes = self.apply(observation(at=T3, validity=["De 01/08/2026 10:00 a 02/11/2026 13:00."]), catalog(T3))
        self.assertEqual({change["kind"] for change in changes}, {"detail_changed", "validity_changed"})

    def test_unknown_fields_survive_updates(self):
        self.state["custom"] = {"keep": True}
        self.state["codes"]["pPS"] = {"custom": ["keep"]}
        self.apply(observation(), catalog())
        self.assertEqual(self.state["custom"], {"keep": True})
        self.assertEqual(self.state["codes"]["pPS"]["custom"], ["keep"])

    def test_health_failure_and_recovery_deduplicate(self):
        plan = observe.make_plan(self.state)
        def summary(unknown):
            return observe.build_summary(self.state, [observation("unknown" if unknown else "found")], catalog(complete=not unknown), plan, [], T1, 1, "")
        self.assertEqual(summary(True)["health_change"], "failure")
        self.assertFalse(summary(True)["should_notify"])
        self.assertEqual(summary(False)["health_change"], "recovery")
        self.assertFalse(summary(False)["should_notify"])


class ScopeTests(unittest.TestCase):
    def test_bounds_and_case_sensitive_codes(self):
        plan = observe.make_plan(observe.new_state())
        self.assertEqual(plan["code_count"], 497)
        self.assertIn("pPa", plan["codes"])
        self.assertIn("pPA", plan["codes"])
        self.assertNotIn("pU0", plan["codes"])
        self.assertEqual(len(set(plan["codes"])), len(plan["codes"]))

    def test_new_prefix_requires_two_distinct_latest_codes(self):
        state = observe.new_state()
        latest = {"offers": [offer("pUa"), offer("pUb"), offer("pVc", catalog=URL + "&offset=96")]}
        plan = observe.make_plan(state, latest)
        self.assertEqual(plan["new_prefixes"], ["pU"])
        self.assertIn("pU0", plan["codes"])
        self.assertNotIn("pV0", plan["codes"])
        self.assertEqual(observe.make_plan(state, {"offers": [offer("pUa"), offer("pUa")]})["new_prefixes"], [])

    def test_new_prefixes_over_limit_require_review(self):
        plan = observe.make_plan(observe.new_state(), {"offers": [offer(code) for code in ["pUa", "pUb", "pVa", "pVb"]]})
        self.assertLessEqual(len(plan["codes"]), 560)
        self.assertTrue(plan["scope_review_required"])
        self.assertIn("prefix_requires_review:pV", plan["scope_reasons"])

    def test_ingressos_require_official_campaign_url(self):
        state = observe.new_state()
        fake = offer("pXa")
        fake["url"] = "https://clube.uol.com.br/cinema/pXa-desconto"
        self.assertNotIn("pXa", observe.make_plan(state, {"offers": [fake]})["codes"])
        fake["url"] = "https://untrusted.example/campanhasdeingresso/pXa-show"
        self.assertNotIn("pXa", observe.make_plan(state, {"offers": [fake]})["codes"])


class ReportTests(unittest.TestCase):
    def export(self, state):
        summary = {
            "date": "2026-10-09", "status": "recorded", "requests": 0,
            "codes_checked": 0, "found": 0, "unknown": 0,
            "limitations": "Primeira observação não é criação.",
            "priority_target": "Alvo", "changes": [], "problems": [],
        }
        with tempfile.TemporaryDirectory() as directory:
            observe.export_reports(directory, state, summary)
            with (Path(directory) / "observations.csv").open(newline="") as stream:
                rows = list(csv.DictReader(stream))
            return rows, (Path(directory) / "report.md").read_text()

    def test_reports_sort_event_dates_descending_unknown_dates_last(self):
        state = observe.new_state()
        state["codes"] = {code: {"first_seen": T1, "title": code} for code in ["pAa", "pBb", "pCc", "pDd", "pEe"]}
        state["config"]["metadata"] = {
            "pAa": {"sortDate": "2026-01-10"},
            "pBb": {"sortDate": "2026-02-30"},
            "pCc": {"sortDate": "2026-10-09"},
            "pEe": {"sortDate": "2027-01-01", "eventName": "Evento futuro", "eventDate": "01/01/2027"},
        }
        rows, report = self.export(state)
        ordered = ["pEe", "pCc", "pAa", "pBb", "pDd"]
        self.assertEqual([row["codigo"] for row in rows], ordered)
        self.assertEqual([report.index(f"`{code}`") for code in ordered], sorted(report.index(f"`{code}`") for code in ordered))
        self.assertEqual(rows[0]["evento"], "Evento futuro")
        self.assertEqual(rows[0]["data"], "01/01/2027")
        self.assertIn("Evento futuro — 01/01/2027; evento futuro", report)
        self.assertIn("pCc — 2026-10-09; evento hoje", report)
        self.assertIn("pAa — 2026-01-10; evento passado", report)
        self.assertEqual(report.count("data do evento não classificada"), 2)

    def test_historical_references_do_not_require_public_observation(self):
        state = observe.new_state()
        state["codes"] = {"pAa": {"first_seen": T1, "title": "Página observada"}, "pBb": {"last_status": "unknown"}}
        state["config"]["metadata"] = {
            "pAa": {"sortDate": "2026-10-30"},
            "pBb": {"sortDate": "2025-10-30", "eventName": "Evento anterior", "referenceSource": "Referência B"},
            "pCc": {"sortDate": "2027-10-30", "eventName": "Evento de referência", "eventDate": "30/10/2027", "referenceSource": "Referência C"},
        }
        before = copy.deepcopy(state)
        rows, report = self.export(state)
        self.assertEqual([row["codigo"] for row in rows], ["pCc", "pAa", "pBb"])
        by_code = {row["codigo"]: row for row in rows}
        self.assertEqual(by_code["pAa"]["tipo_registro"], "observação pública")
        self.assertEqual(by_code["pCc"]["tipo_registro"], "referência histórica")
        self.assertEqual(by_code["pCc"]["fonte_referencia"], "Referência C")
        self.assertEqual(by_code["pCc"]["primeira_aparicao"], "")
        public, historical = report.split("## Referências históricas sem observação pública confirmada")
        self.assertIn("`pAa`", public)
        self.assertNotIn("`pBb`", public)
        self.assertNotIn("`pCc`", public)
        self.assertLess(historical.index("`pCc`"), historical.index("`pBb`"))
        self.assertIn("referência histórica: Referência C; sem observação pública confirmada pelo coletor", historical)
        self.assertEqual(state, before)

    def test_legacy_event_fields_and_csv_formula_protection_remain(self):
        state = observe.new_state()
        state["codes"]["pAa"] = {"first_seen": T1, "title": "=Título"}
        state["config"]["metadata"]["pAa"] = {"event": "@Evento", "date": "30/10/2026", "referenceSource": "=Fonte"}
        rows, report = self.export(state)
        self.assertEqual(rows[0]["titulo"], "'=Título")
        self.assertEqual(rows[0]["evento"], "'@Evento")
        self.assertEqual(rows[0]["data"], "30/10/2026")
        self.assertEqual(rows[0]["fonte_referencia"], "'=Fonte")
        self.assertIn("@Evento — 30/10/2026", report)


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        state = observe.new_state()
        state["config"].update(prefixes=[], known_codes=["pPS"])
        (self.directory / "state.json").write_text(json.dumps(state))

    def tearDown(self):
        self.temp.cleanup()

    def test_same_local_day_repeats_without_network(self):
        first = observe.run(self.directory, FakeProbe(), T1)
        snapshot = next((self.directory / "snapshots").glob("*.json"))
        original = snapshot.read_bytes()
        second_backend = FakeProbe()
        second = observe.run(self.directory, second_backend, "2026-10-10T01:59:00Z")
        self.assertEqual(first["status"], "recorded")
        self.assertEqual(second["status"], "already_recorded")
        self.assertEqual(second_backend.calls, [])
        self.assertEqual(snapshot.read_bytes(), original)

    def test_partial_is_recorded_and_never_automatically_retried(self):
        self.assertEqual(observe.run(self.directory, FakeProbe(unknown=True), T1)["status"], "partial")
        second_backend = FakeProbe()
        self.assertEqual(observe.run(self.directory, second_backend, T1)["status"], "already_recorded")
        self.assertEqual(second_backend.calls, [])

    def test_orphan_snapshot_recovers_without_network(self):
        observe.run(self.directory, FakeProbe(), T1)
        (self.directory / "state.json").unlink()
        backend = FakeProbe()
        self.assertEqual(observe.run(self.directory, backend, T1)["status"], "already_recorded")
        self.assertEqual(backend.calls, [])
        self.assertIn("2026-10-09", json.loads((self.directory / "state.json").read_text())["runs"])

    def test_lock_conflict_does_not_modify_state(self):
        before = (self.directory / "state.json").read_bytes()
        with (self.directory / ".lock").open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(RuntimeError, "Another observation"):
                observe.run(self.directory, FakeProbe(), T1)
        self.assertEqual((self.directory / "state.json").read_bytes(), before)
        self.assertFalse((self.directory / "snapshots").exists())

    def test_plan_does_not_create_data_directory(self):
        missing = self.directory / "missing"
        observe.make_plan(observe.load_state(missing))
        self.assertFalse(missing.exists())


if __name__ == "__main__":
    unittest.main()
