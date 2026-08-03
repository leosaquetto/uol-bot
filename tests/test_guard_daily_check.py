import io
import json
import os
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import guard_daily_check
from legacy_safety import REDACTED, redact_sensitive_text, sanitize_audit_value


UTC = timezone.utc


class LegacySafetyTests(unittest.TestCase):
    def test_redacts_credentials_from_text_and_audit_values(self) -> None:
        secret = "github_pat_this_is_a_test_secret_123456"
        sample = (
            f"Bearer {secret} "
            "https://api.telegram.org/bot123456:ABC_def/sendMessage "
            "https://example.test/path?token=query-secret&ok=1 "
            "https://alice:password@example.test/private"
        )
        with patch.dict(os.environ, {"GITHUB_TOKEN": secret}, clear=False):
            redacted = redact_sensitive_text(sample)
            audit = sanitize_audit_value(
                {"authorization": secret, "error": sample, "nested": {"api_key": "abc123"}}
            )

        self.assertNotIn(secret, redacted)
        self.assertNotIn("123456:ABC_def", redacted)
        self.assertNotIn("query-secret", redacted)
        self.assertNotIn("alice:password", redacted)
        self.assertEqual(audit["authorization"], REDACTED)
        self.assertEqual(audit["nested"]["api_key"], REDACTED)
        self.assertNotIn(secret, json.dumps(audit))


class GuardDailyCheckTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.now = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def args(self) -> Namespace:
        return Namespace(
            decision_file=str(self.root / "decision.json"),
            audit_file=str(self.root / "audit.jsonl"),
            pipeline_audit_file=str(self.root / "pipeline.jsonl"),
            lock_file=str(self.root / "lock.json"),
            lock_timeout_sec=1800,
            log_file=str(self.root / "guard.log"),
            alerts_file=str(self.root / "alerts.jsonl"),
            report_file=str(self.root / "report.json"),
            duplicate_window_min=60,
        )

    def write_decision(self, **patch_values) -> None:
        decision = {
            "decision": "skip_fallback",
            "run_fallback": False,
            "reason": "pipeline_audit_recent",
            "recorded_at_utc": "2026-08-02T11:55:00Z",
            "pipeline_audit_age_min": 5,
            "stale_window_min": 30,
            "drift_margin_min": 2,
            "ssh_probe": {"ok": True, "source": "ssh"},
        }
        decision.update(patch_values)
        Path(self.args().decision_file).write_text(json.dumps(decision), encoding="utf-8")

    def test_healthy_report_uses_runbook_field_names(self) -> None:
        self.write_decision()

        status, summary, alerts = guard_daily_check.evaluate(self.args(), current_time=self.now)

        self.assertEqual(status, "OK")
        self.assertEqual(alerts, [])
        self.assertIs(summary["mac_online"], True)
        self.assertIs(summary["pipeline_fresh"], True)
        self.assertIs(summary["lock_stale"], False)
        self.assertNotIn("lock_stuck", summary)
        self.assertEqual(summary["pipeline_age_source"], "decision")

    def test_offline_and_stale_lock_are_alerts(self) -> None:
        self.write_decision(
            decision="run_fallback",
            run_fallback=True,
            reason="mac_offline_or_inaccessible",
            pipeline_audit_age_min=None,
            ssh_probe={"ok": False, "error": "offline"},
        )
        old_start_ms = int((self.now.timestamp() - 1900) * 1000)
        Path(self.args().lock_file).write_text(json.dumps({"started_ts": old_start_ms}), encoding="utf-8")

        status, summary, _ = guard_daily_check.evaluate(self.args(), current_time=self.now)

        self.assertEqual(status, "ALERTA")
        self.assertIs(summary["mac_online"], False)
        self.assertIs(summary["pipeline_fresh"], False)
        self.assertIs(summary["lock_stale"], True)
        self.assertIn("mac_offline", summary["issues"])
        self.assertIn("pipeline_stale", summary["issues"])
        self.assertIn("lock_stale", summary["issues"])

    def test_invalid_lock_is_reported_without_crashing(self) -> None:
        self.write_decision()
        Path(self.args().lock_file).write_text('{"started_ts":"not-a-number"}', encoding="utf-8")

        status, summary, _ = guard_daily_check.evaluate(self.args(), current_time=self.now)

        self.assertEqual(status, "ALERTA")
        self.assertIs(summary["lock_stale"], False)
        self.assertIs(summary["lock_valid"], False)
        self.assertIn("lock_invalid", summary["issues"])

    def test_skip_divergence_ignores_execution_before_decision(self) -> None:
        decision = {
            "decision": "skip_fallback",
            "run_fallback": False,
            "recorded_at_utc": "2026-08-02T11:00:00Z",
        }
        before = [{"event": "fallback_executed", "run_fallback": True, "at_utc": "2026-08-02T10:00:00Z"}]
        after = before + [
            {"event": "fallback_executed", "run_fallback": True, "at_utc": "2026-08-02T11:01:00Z"}
        ]

        self.assertEqual(
            guard_daily_check.detect_audit_anomalies(before, decision, 60, current_time=self.now),
            [],
        )
        alerts = guard_daily_check.detect_audit_anomalies(after, decision, 60, current_time=self.now)
        self.assertEqual([item["alert_type"] for item in alerts], ["decision_skip_with_fallback_execution"])

    def test_cli_writes_report_and_returns_nonzero_for_alert(self) -> None:
        args = self.args()
        with patch("guard_daily_check.now_utc", return_value=self.now), redirect_stdout(io.StringIO()):
            exit_code = guard_daily_check.main(
                [
                    "--decision-file", args.decision_file,
                    "--audit-file", args.audit_file,
                    "--pipeline-audit-file", args.pipeline_audit_file,
                    "--lock-file", args.lock_file,
                    "--log-file", args.log_file,
                    "--alerts-file", args.alerts_file,
                    "--report-file", args.report_file,
                ]
            )

        report = json.loads(Path(args.report_file).read_text(encoding="utf-8"))
        self.assertEqual(exit_code, 1)
        self.assertEqual(report["status"], "ALERTA")
        self.assertEqual(report["report_schema_version"], 2)
        self.assertIn("decision_missing", report["issues"])

    def test_cli_returns_zero_for_healthy_state(self) -> None:
        self.write_decision()
        args = self.args()
        with patch("guard_daily_check.now_utc", return_value=self.now), redirect_stdout(io.StringIO()):
            exit_code = guard_daily_check.main(
                [
                    "--decision-file", args.decision_file,
                    "--audit-file", args.audit_file,
                    "--pipeline-audit-file", args.pipeline_audit_file,
                    "--lock-file", args.lock_file,
                    "--log-file", args.log_file,
                    "--alerts-file", args.alerts_file,
                    "--report-file", args.report_file,
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(Path(args.report_file).read_text())["status"], "OK")

    def test_legacy_tls_never_disables_certificate_validation(self) -> None:
        source = (Path(__file__).parents[1] / "github_scraper.py").read_text(encoding="utf-8")
        self.assertNotRegex(source, r"verify\s*=\s*False")
        self.assertNotIn("fallback sem verify", source)
        self.assertIn("verify=certifi.where()", source)

    def test_scriptable_entrypoints_record_real_fallback_execution(self) -> None:
        root = Path(__file__).parents[1]
        for filename in ("scriptable_uol_parte1.js", "scriptable_uol_fallback_ingressos_single.js"):
            source = (root / filename).read_text(encoding="utf-8")
            self.assertIn('appendFallbackAudit("fallback_executed"', source)
            self.assertIn("run_fallback: true", source)


if __name__ == "__main__":
    unittest.main()
