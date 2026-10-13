import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import observe
import pattern_report


T1 = "2026-10-09T15:00:00Z"
T2 = "2026-10-10T15:00:00Z"
T3 = "2026-10-11T15:00:00Z"


def state(codes=None, metadata=None):
    return {"codes": codes or {}, "config": {"metadata": metadata or {}}}


def record(**values):
    return {"first_seen": T1, "last_seen": T3, "last_status": "found", "last_checked": T3,
            "last_conclusive_page_status": "found", "last_conclusive_page_at": T3,
            "listing_state": "not_listed", "listing_observed_at": T3, "transitions": [], **values}


def show(**values):
    return {"type": "Show", "eventName": "Artista público", "eventDate": "30/10, 21h",
            "sortDate": "2026-10-30", "sortBasis": "Ano inferido apenas para ordem", **values}


def report(value, day="2026-10-11"):
    return pattern_report.build_patterns(value, {"date": day, "observed_at": T3, "status": "partial", "catalog_complete": False})


class PatternTests(unittest.TestCase):
    def test_counts_use_sample_not_runtime_clock_and_preserve_unknown(self):
        data = state({"pAa": record(), "pAb": record(last_status="unknown"), "pAc": {"last_status": "absent"},
                      "pAd": record(first_listed=T1), "pAe": record(first_listed=T1, listing_state="listed")},
                     {"pAa": show()})
        before = copy.deepcopy(data)
        result = report(data)
        self.assertEqual(result["counts"], {"tracked_codes": 5, "confirmed_pages_ever": 4, "latest_found": 3,
                         "latest_absent": 1, "latest_unknown": 1, "confirmed_never_observed_listed": 2,
                         "confirmed_listed_then_removed": 1, "latest_found_off_list": 2,
                         "identified_public_shows": 1, "historical_show_references": 0})
        self.assertEqual(result["as_of_date"], "2026-10-11")
        self.assertEqual(result["current_or_future_shows"][0]["date_group"], "current_or_future")
        self.assertEqual([item["code"] for item in result["found_off_list"]], ["pAa", "pAd"])
        self.assertEqual(data, before)

    def test_artist_not_inferred_from_title_venue_or_code(self):
        data = state({"pAa": record(title="Artista no título"), "pAb": record(), "pAc": record(), "pAd": record()},
                     {"pAa": {"eventVenue": "Arena", "sortDate": "2026-10-30"},
                      "pAb": show(type="Teatro"), "pAc": show(eventName=""), "pAd": show()})
        result = report(data)
        self.assertEqual([item["code"] for item in result["current_or_future_shows"]], ["pAd"])

    def test_withdrawn_show_can_keep_found_page(self):
        transition = {"kind": "unlisted", "previous_observed_at": T2, "observed_at": T3}
        data = state({"pAa": record(first_listed=T1, last_listed=T2, transitions=[transition])}, {"pAa": show()})
        item = report(data)["current_or_future_shows"][0]
        self.assertEqual(item["last_page_status"], "found")
        self.assertEqual(item["listing_state"], "not_listed")
        self.assertEqual(item["timeline"][0]["observation_gap_seconds"], 86400)
        episode = item["listing_presence_episodes"][0]
        self.assertEqual(episode["minimum_seconds"], 86400)
        self.assertIsNone(episode["maximum_seconds"])

    def test_first_seen_not_validity_or_publication_and_preserves_year_precision(self):
        values = ["Benefício válido de 01/08/2026 10:00 até 31/10/2026 13:00."]
        data = state({"pAa": record(first_listed=T2, validity=values)}, {"pAa": show()})
        item = report(data)["current_or_future_shows"][0]
        self.assertEqual(item["first_seen"]["by"], T1)
        self.assertEqual(item["first_seen"]["confidence"], "baseline")
        self.assertEqual(item["publication_lead"], "not_established")
        self.assertNotIn("first_observation_offset_seconds", item)
        self.assertEqual(item["validity_raw"], values)
        self.assertEqual(item["validity_start"], {"value": "01/08/2026 10:00", "precision": "minute", "timezone": "not_published"})
        self.assertEqual(item["event_date_raw"], "30/10, 21h")
        self.assertEqual(item["sort_date_basis"], "Ano inferido apenas para ordem")
        self.assertIsNone(pattern_report._validity_start(["Benefício válido de 01/08 até 31/10."]))
        self.assertEqual(pattern_report._validity_start(["01/08/2026"])["precision"], "day")

    def test_epoch_comparison_handles_timezones_without_fake_bound(self):
        data = state({"pAa": record(first_seen="2026-10-09T12:00:00-03:00", first_listed=T1,
                      transitions=[{"kind": "first_seen", "previous_observed_at": "2026-10-08T12:00:00-03:00", "observed_at": T1}])}, {"pAa": show()})
        item = report(data)["current_or_future_shows"][0]
        self.assertEqual(item["first_seen"]["observation_gap_seconds"], 86400)
        self.assertEqual(pattern_report._interval("listed", T3, T1)["confidence"], "unknown")
        self.assertIsNone(pattern_report._interval("listed", T3, T1)["observation_gap_seconds"])

    def test_unknown_dates_and_historical_references_do_not_become_observations(self):
        data = state({"pAa": record(), "pAb": record()},
                     {"pAa": show(sortDate="2026-02-30"), "pAb": show(sortDate=""),
                      "pAc": show(historical=True, sortDate="2025-10-30", instagramReferences=[{"story_id": "private-id"}])})
        result = report(data)
        self.assertEqual([item["code"] for item in result["unknown_date_shows"]], ["pAa", "pAb"])
        reference = result["past_shows"][0]
        self.assertEqual(reference["evidence_type"], "historical_reference")
        self.assertIsNone(reference["first_seen"]["by"])
        self.assertEqual(result["counts"]["confirmed_pages_ever"], 2)
        self.assertNotIn("private-id", json.dumps(result))

    def test_code_order_is_case_sensitive_hypothesis_and_gaps_are_not_free_codes(self):
        data = state({"pQa": record(validity=["01/10/2026 10:00"]), "pQc": record(validity=["02/10/2026 10:00"]),
                      "pQ0": record(validity=["03/10/2026 10:00"]), "pQA": record(validity=["02/10/2026 10:00"]),
                      "pQb": {"last_status": "absent"}})
        result = report(data)["code_order"]
        self.assertEqual(result["status"], "hypothesis")
        self.assertEqual(result["ordered_confirmed_codes"], ["pQa", "pQc", "pQ0", "pQA"])
        self.assertIn("pQb", result["missing_between_confirmed_codes"])
        self.assertNotIn("pQa", result["missing_between_confirmed_codes"])
        self.assertIn("Not proof", result["gap_meaning"])
        self.assertIn("another benefit", result["gap_meaning"])

    def test_reappearance_and_relisting_keep_multiple_episode_bounds(self):
        events = [{"kind": "first_seen", "previous_observed_at": "2026-10-08T15:00:00Z", "observed_at": T1},
                  {"kind": "page_gone", "previous_observed_at": T1, "observed_at": T2},
                  {"kind": "page_reappeared", "previous_observed_at": T2, "observed_at": T3},
                  {"kind": "listed", "previous_observed_at": None, "observed_at": T1},
                  {"kind": "unlisted", "previous_observed_at": T1, "observed_at": T2},
                  {"kind": "listed", "previous_observed_at": T2, "observed_at": T3}]
        item = report(state({"pAa": record(first_listed=T1, transitions=events)}, {"pAa": show()}))["current_or_future_shows"][0]
        self.assertEqual(item["page_reappearances"], 1)
        self.assertEqual(item["relistings"], 1)
        self.assertEqual(item["page_presence_episodes"][0]["maximum_seconds"], 172800)
        self.assertEqual(item["listing_presence_episodes"][0]["minimum_seconds"], 0)

    def test_bootstrap_listing_is_not_replaced_by_later_relisting(self):
        events = [{"kind": "unlisted", "previous_observed_at": T1, "observed_at": T2},
                  {"kind": "listed", "previous_observed_at": T2, "observed_at": T3}]
        item = report(state({"pAa": record(first_listed=T1, transitions=events)}, {"pAa": show()}))["current_or_future_shows"][0]
        self.assertEqual(item["first_listed"]["by"], T1)
        self.assertEqual(item["first_listed"]["confidence"], "baseline")
        self.assertEqual(item["relistings"], 1)
        self.assertEqual(item["listing_presence_episodes"][0]["entry"]["by"], T1)

    def test_cli_prints_only_and_does_not_change_input(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "state.json").write_text(json.dumps(state({"pAa": record()}, {"pAa": show()})))
            (root / "summary.json").write_text(json.dumps({"date": "2026-10-11"}))
            before = {path.name: path.read_bytes() for path in root.iterdir()}
            result = subprocess.run([sys.executable, str(Path(pattern_report.__file__)), "--data-dir", directory, "--format", "markdown"], capture_output=True, text=True, check=True)
            self.assertIn("Padrões observados", result.stdout)
            self.assertIn("baseline", result.stdout)
            self.assertEqual({path.name: path.read_bytes() for path in root.iterdir()}, before)

    def test_story_order_requires_bounded_windows_and_exact_public_url(self):
        url = "https://clube.uol.com.br/campanhasdeingresso/pAa-show"
        data = state({"pAa": record(url=url, first_listed=T3, transitions=[
            {"kind": "first_seen", "previous_observed_at": "2026-10-08T15:00:00Z", "observed_at": T1},
            {"kind": "listed", "previous_observed_at": T2, "observed_at": T3}])}, {"pAa": show()})
        story = {"code": "pAa", "canonicalUrl": url, "instagramPublishedAt": "2026-10-09T18:00:00Z",
                 "instagramFirstSeenAt": "2026-10-09T18:05:00Z", "collectionTrigger": "poll", "baseline": False,
                 "excludedReason": None, "pushCorrelation": "without_corresponding_push", "outboxStatus": "sent",
                 "targets": {"discord": {"status": "sent", "imageConfirmed": True, "private_id": "hidden"}}}
        history = {"status": "available", "records": [story], "capturedAt": T3}
        result = pattern_report.build_patterns(data, {"date": "2026-10-11"}, history)
        reference = result["current_or_future_shows"][0]["instagram_references"][0]
        self.assertEqual(reference["page_story_order"], "page_before_story")
        self.assertEqual(reference["catalog_story_order"], "story_before_catalog")
        self.assertEqual(reference["collectionTrigger"], "poll")
        self.assertNotIn("hidden", json.dumps(result))
        self.assertEqual(reference["targets"]["discord"], {"status": "sent", "imageConfirmed": True})
        self.assertEqual(result["instagram_history"]["matched_show_references"], 1)
        mismatch = copy.deepcopy(history)
        mismatch["records"][0]["canonicalUrl"] += "?tracking=1"
        self.assertEqual(pattern_report.build_patterns(data, {"date": "2026-10-11"}, mismatch)["current_or_future_shows"][0]["instagram_references"], [])

    def test_story_baseline_missing_coverage_and_overlap_remain_uncertain(self):
        url = "https://clube.uol.com.br/campanhasdeingresso/pAa-show"
        data = state({"pAa": record(url=url, transitions=[{"kind": "first_seen", "previous_observed_at": T1, "observed_at": T3}])}, {"pAa": show()})
        story = {"code": "pAa", "canonicalUrl": url, "instagramPublishedAt": T2, "baseline": False}
        for values in ({}, {"baseline": True}, {"excludedReason": "coverage_metadata_missing"}, {"instagramPublishedAt": "2026-10-12T15:00:00Z", "baseline": None}):
            history = {"status": "partial", "records": [{**story, **values}]}
            item = pattern_report.build_patterns(data, {"date": "2026-10-11"}, history)["current_or_future_shows"][0]
            self.assertEqual(item["instagram_references"][0]["page_story_order"], "order_uncertain")
            self.assertEqual(item["instagram_references"][0]["catalog_story_order"], "order_uncertain")

    def test_missing_story_history_is_unavailable_not_zero_observations(self):
        result = report(state())
        self.assertEqual(result["instagram_history"]["status"], "unavailable")
        self.assertIsNone(result["instagram_history"]["record_count"])
        result = pattern_report.build_patterns(state(), {"date": "2026-10-11"}, {"status": "unavailable", "records": []})
        self.assertIsNone(result["instagram_history"]["record_count"])

    def test_non_show_story_appears_without_artist_inference(self):
        url = "https://clube.uol.com.br/campanhasdeingresso/pQ5-musical"
        data = state({"pQ5": record(url=url, title="Título público genérico")},
                     {"pQ5": {"type": "Teatro musical", "eventName": "Nome documentado", "url": url}})
        stories = {"status": "partial", "records": [{"code": "pQ5", "canonicalUrl": url,
                   "instagramPublishedAt": T2, "collectionTrigger": "push", "baseline": True,
                   "pushCorrelation": "generic_profile_signal_uncertain", "outboxStatus": "sent",
                   "targets": {"discord": {"status": "sent", "imageConfirmed": None}}}]}
        result = pattern_report.build_patterns(data, {"date": "2026-10-11"}, stories)
        self.assertEqual(result["current_or_future_shows"], [])
        observation = result["instagram_history"]["observations"][0]
        self.assertEqual(observation["event_name"], "Nome documentado")
        self.assertEqual(observation["event_type"], "Teatro musical")
        self.assertEqual(observation["page_story_order"], "order_uncertain")
        self.assertIn("`pQ5`", pattern_report.render_markdown(result))


class ExportIntegrationTests(unittest.TestCase):
    def data(self):
        value = observe.new_state()
        value["codes"] = {"pAa": record(url="https://clube.uol.com.br/campanhasdeingresso/pAa-show")}
        value["config"]["metadata"] = {"pAa": show()}
        summary = {"date": "2026-10-11", "status": "recorded", "requests": 0, "codes_checked": 1,
                   "found": 1, "unknown": 0, "limitations": "Observação não prova publicação.",
                   "priority_target": "Alvo", "changes": [], "problems": []}
        return value, summary

    def test_export_generates_patterns_and_originals_without_history(self):
        with tempfile.TemporaryDirectory() as directory:
            value, summary = self.data()
            before = copy.deepcopy(value)
            observe.export_reports(directory, value, summary)
            root = Path(directory)
            self.assertEqual({path.name for path in root.iterdir()}, {"observations.csv", "report.md", "patterns.json", "patterns.md"})
            patterns = json.loads((root / "patterns.json").read_text())
            self.assertEqual(patterns["as_of_date"], summary["date"])
            self.assertEqual(patterns["instagram_history"]["status"], "unavailable")
            self.assertIn("Padrões observados", (root / "patterns.md").read_text())
            self.assertIn("`pAa`", (root / "report.md").read_text())
            self.assertEqual(value, before)

    def test_corrupt_or_wrong_type_history_does_not_break_exports_or_modify_history(self):
        for content in ("{bad json", "[]", '{"records": {}}', '{"records": [null]}'):
            with self.subTest(content=content), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                history_path = root / "instagram-history.json"
                history_path.write_text(content)
                value, summary = self.data()
                observe.export_reports(directory, value, summary)
                patterns = json.loads((root / "patterns.json").read_text())
                self.assertEqual(patterns["instagram_history"]["status"], "unavailable")
                self.assertEqual(patterns["instagram_history"]["reason"], "local_history_unreadable")
                self.assertTrue((root / "observations.csv").is_file())
                self.assertIn("`pAa`", (root / "report.md").read_text())
                self.assertEqual(history_path.read_text(), content)

    def test_export_uses_sanitized_optional_history(self):
        with tempfile.TemporaryDirectory() as directory:
            value, summary = self.data()
            history = {"schemaVersion": 1, "status": "available", "records": [{"code": "pAa",
                       "canonicalUrl": value["codes"]["pAa"]["url"], "instagramPublishedAt": T2,
                       "instagramFirstSeenAt": T3, "collectionTrigger": "poll", "baseline": False,
                       "excludedReason": None, "pushCorrelation": "without_corresponding_push", "targets": {}}]}
            root = Path(directory)
            (root / "instagram-history.json").write_text(json.dumps(history))
            observe.export_reports(directory, value, summary)
            patterns = json.loads((root / "patterns.json").read_text())
            self.assertEqual(patterns["instagram_history"]["status"], "available")
            self.assertEqual(patterns["instagram_history"]["observations"][0]["code"], "pAa")


if __name__ == "__main__":
    unittest.main()
