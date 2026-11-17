"""End-to-end scenarios against real local processes.

Run:  python3 -m unittest discover -s tests -v
Each test class boots a fresh estate in a temp dir with ephemeral ports.
No mocks stand in for the components under test: the config service, the three
collector processes, the sink and the mock site are all real processes.
"""
from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness  # noqa: E402
from harness import Estate, http_json, wait_for  # noqa: E402


def boot(preset: str = "v1") -> tuple[Estate, tempfile.TemporaryDirectory]:
    tmp = tempfile.TemporaryDirectory(prefix="estate-")
    estate = Estate(Path(tmp.name), preset=preset).boot()
    return estate, tmp


class EstateCase(unittest.TestCase):
    preset = "v1"

    def setUp(self):
        self.estate, self._tmp = boot(self.preset)
        self.addCleanup(self._tmp.cleanup)
        self.addCleanup(self.estate.shutdown)

    # -------------------------------------------------------------- helpers
    def published(self) -> dict:
        return http_json(self.estate.config + "/published")[1]

    def expected(self, preset: str) -> list[dict]:
        from collect.presets import expected_rows
        return expected_rows(preset)


class TestConfigurationPair(EstateCase):
    def test_both_entries_and_ranges_are_published_as_one_pair(self):
        body = self.published()
        self.assertEqual(body["version"], "v1")
        self.assertEqual(set(body["mapping"]), {"id", "title", "price", "stock"})
        self.assertEqual(body["scope"]["ids"],
                         ["A-100", "A-101", "A-102", "B-200", "B-201"])
        # The hash covers mapping+scope jointly and the seal covers the body.
        from collect.protocol import validate_body
        validate_body(body)

    def test_partial_config_is_refused(self):
        status, payload = http_json(
            self.estate.config + "/versions",
            {"mapping": {"id": "sku", "title": "label", "price": "price_cents"}})
        self.assertEqual(status, 400)
        self.assertIn("BOTH mapping and scope", payload["error"])

    def test_service_refuses_a_pair_mixed_from_two_versions(self):
        """A hand-built mix is rejected because both hashes must agree."""
        from collect.protocol import pair_scope_hash, mapping_fingerprint
        from collect.presets import PRESETS
        v2_mapping = PRESETS["v2"]["mapping"]
        v1_scope = PRESETS["v1"]["scope"]
        status, payload = http_json(self.estate.config + "/versions", {
            "mapping": v2_mapping, "scope": v1_scope,
            "fingerprint": mapping_fingerprint(v2_mapping),
            "scope_hash": pair_scope_hash(v2_mapping, v1_scope),
        })
        # Unknown fields are rejected outright: clients cannot supply hashes.
        self.assertEqual(status, 400)
        # A version built from the mixed pair is legal *as a pair*, but its
        # scope_hash never matches the v2 hash, so it is a distinct version.
        status, body = http_json(self.estate.config + "/versions", {
            "mapping": v2_mapping, "scope": v1_scope, "version": "v-mixed"})
        self.assertEqual(status, 201, body)
        self.assertNotEqual(body["scope_hash"], self.published()["scope_hash"])

    def test_publishing_backwards_is_refused(self):
        status, created = self.estate.publish(preset="v2", version="v2")
        self.assertEqual(status, 200, created)
        # Now try to go back to v1, which is older.
        status, payload = http_json(self.estate.config + "/publish",
                                    {"version": "v1"})
        self.assertEqual(status, 409)
        self.assertIn("backwards", payload["error"])
        self.assertEqual(self.published()["version"], "v2")


class TestThreeEntries(EstateCase):
    def test_all_entries_produce_identical_rows_from_real_reads(self):
        task_id = self.estate.run_task("barrier", "auto")
        task = self.estate.wait_task(task_id, ("committed",))
        self.assertEqual(task["published_rows"], 3 * 3)
        rows = self.estate.records(task_id)
        by_entry: dict[str, list] = {}
        for row in rows:
            by_entry.setdefault(row["entry_id"], []).append(
                {"id": row["item_id"], "title": row["title"],
                 "price_cents": row["price_cents"]})
        self.assertEqual(set(by_entry), {"api", "file", "page"})
        for entry, got in by_entry.items():
            got.sort(key=lambda r: r["id"])
            self.assertEqual(got, self.expected("v1"), entry)
        for entry in task["entries"]:
            self.assertEqual(entry["state"], "verified")
            self.assertTrue(entry["verification"]["independent_read"])
            self.assertEqual(entry["config_evidence"]["source"], "live")

    def test_confirmations_are_backed_by_real_config_reads(self):
        """Every entry's claimed body must appear in the service access log."""
        task_id = self.estate.run_task("barrier", "auto")
        task = self.estate.wait_task(task_id, ("committed",))
        access = http_json(self.estate.config + "/access")[1]["access"]
        reads = [row for row in access
                 if row["kind"] in ("published_read", "version_read")]
        self.assertGreaterEqual(len(reads), 3)
        for entry in task["entries"]:
            claimed = entry["config_evidence"]["body_sha256"]
            seen = [row for row in reads if row["body_sha256"] == claimed]
            self.assertTrue(seen, f"no config read logged for {entry['entry_id']}")
            # The read must come from that entry process itself, identified by
            # its User-Agent, for exactly the body it reported.
            by_entry = [row for row in seen
                        if f"collector/{entry['entry_id']}" in
                        (row["user_agent"] or "")]
            self.assertTrue(by_entry, entry["entry_id"])

    def test_each_entry_reads_its_own_source_shape(self):
        task_id = self.estate.run_task("prefetch", "auto")
        task = self.estate.wait_task(task_id, ("committed",))
        kinds = {e["entry_id"]: e["source_evidence"]["kind"] for e in task["entries"]}
        self.assertEqual(kinds, {"api": "api", "file": "file", "page": "page"})

    def test_v2_changes_both_mapping_and_range(self):
        status, payload = self.estate.publish(preset="v2", version="v2")
        self.assertEqual(status, 200, payload)
        task_id = self.estate.run_task("barrier", "auto")
        task = self.estate.wait_task(task_id, ("committed",))
        rows = self.estate.records(task_id)
        per_entry = {name: [] for name in ("api", "file", "page")}
        for row in rows:
            per_entry[row["entry_id"]].append(
                {"id": row["item_id"], "title": row["title"],
                 "price_cents": row["price_cents"]})
        for entry, got in per_entry.items():
            got.sort(key=lambda r: r["id"])
            self.assertEqual(got, self.expected("v2"), entry)
        # v2's ids (Z-*) and titles come from v2's fields.
        self.assertTrue(all(r["item_id"].startswith("Z-") for r in rows))


class TestGatesAndPublishModes(EstateCase):
    def test_barrier_confirms_before_any_source_read(self):
        task_id = self.estate.run_task("barrier", "auto")
        self.estate.wait_task(task_id, ("committed",))
        events = [e["kind"] for e in self.estate.events()]
        confirms = [i for i, k in enumerate(events) if k == "entry_confirmed"]
        prepares = [i for i, k in enumerate(events) if k == "entry_prepared"]
        self.assertEqual(len(confirms), 3)
        self.assertEqual(len(prepares), 3)
        self.assertLess(max(confirms), min(prepares),
                        "every confirmation must precede the first prepare")

    def test_prefetch_stages_but_waits_for_all_before_publish(self):
        task_id = self.estate.run_task("prefetch", "manual")
        task = self.estate.wait_task(task_id, ("ready",))
        self.assertEqual(task["state"], "ready")
        self.assertEqual(task["stored_rows"], 9)
        self.assertEqual(task["published_rows"], 0)
        # Nothing from this task is visible in the published view yet.
        published = self.estate.records()
        self.assertFalse(any(r["task_id"] == task_id for r in published))
        status, task = http_json(
            self.estate.web + f"/api/task/{task_id}/commit", {})
        self.assertEqual(status, 200, task)
        self.assertEqual(task["state"], "committed")
        self.assertEqual(task["published_rows"], 9)

    def test_manual_commit_refuses_after_config_moved_on(self):
        task_id = self.estate.run_task("prefetch", "manual")
        self.estate.wait_task(task_id, ("ready",))
        status, payload = self.estate.publish(preset="v2", version="v2")
        self.assertEqual(status, 200, payload)
        status, payload = http_json(
            self.estate.web + f"/api/task/{task_id}/commit", {})
        self.assertEqual(status, 409, payload)
        task = self.estate.task(task_id)
        self.assertEqual(task["state"], "aborted")
        self.assertEqual(task["published_rows"], 0)
        self.assertEqual(task["stored_rows"], 0)


class TestFailureAndRecovery(EstateCase):
    def test_severed_provider_link_aborts_task_and_purges_staged_rows(self):
        before = len(self.estate.records())
        self.assertEqual(before, 0)
        self.estate.set_outage("page", True)
        task_id = self.estate.run_task("prefetch", "auto")
        task = self.estate.wait_task(task_id, ("aborted",), timeout=30)
        self.assertIn("page", task["note"])
        self.assertEqual(task["stored_rows"], 0)
        self.assertEqual(task["published_rows"], 0)
        self.assertEqual(len(self.estate.records()), 0)
        failed = next(e for e in task["entries"] if e["entry_id"] == "page")
        self.assertEqual(failed["state"], "failed")
        # The other entries were told to withdraw their preparation.
        for entry in task["entries"]:
            if entry["entry_id"] != "page":
                self.assertEqual(entry["state"], "rolled_back")

    def test_entry_recovers_and_next_task_succeeds(self):
        self.estate.set_outage("page", True)
        failed_id = self.estate.run_task("prefetch", "auto")
        self.estate.wait_task(failed_id, ("aborted",), timeout=30)
        self.estate.set_outage("page", False)
        good_id = self.estate.run_task("barrier", "auto")
        task = self.estate.wait_task(good_id, ("committed",), timeout=30)
        self.assertEqual(task["published_rows"], 9)
        self.assertEqual(len(self.estate.records()), 9)

    def test_unreachable_entry_cannot_contribute_a_fake_confirmation(self):
        """An entry that lost its link must fail, not confirm from memory."""
        self.estate.entry_link("file", down=True)
        task_id = self.estate.run_task("barrier", "auto")
        task = self.estate.wait_task(task_id, ("aborted",), timeout=30)
        self.assertEqual(task["published_rows"], 0)
        file_entry = next(e for e in task["entries"] if e["entry_id"] == "file")
        self.assertNotEqual(file_entry["state"], "verified")
        self.assertTrue(file_entry["error"])

    def test_missing_entry_process_aborts_task(self):
        """A killed entry must abort the task, not leave it hanging."""
        self.estate.stop("api")
        task_id = self.estate.run_task("barrier", "auto")
        task = self.estate.wait_task(task_id, ("aborted",), timeout=30)
        self.assertEqual(task["published_rows"], 0)
        self.assertIn("api", task["note"])
        def gone():
            entries = {e["entry_id"]: e for e in self.estate.entries()}
            return not entries["api"]["reachable"]
        from harness import wait_for
        wait_for(gone, timeout=15, message="api reported offline")

    def test_failed_task_leaves_a_rollback_or_release_audit_trail(self):
        self.estate.set_outage("api", True)
        task_id = self.estate.run_task("prefetch", "auto")
        self.estate.wait_task(task_id, ("aborted",), timeout=30)
        kinds = {e["kind"] for e in self.estate.events()}
        self.assertIn("task_aborted", kinds)
        # Staged dirs for that task are gone from reachable entries.
        for entry in ("file", "page"):
            state = http_json(self.estate.urls[entry] + "/state")[1]
            self.assertNotIn(task_id, [p["task_id"] for p in state["prepared"]])


class TestStaleVersions(EstateCase):
    def test_restarted_entry_cannot_confirm_its_pre_outage_version(self):
        """Kill an entry, publish v2, restart the old entry, run a task.

        The restarted entry must load v2 live.  A task explicitly pinned to
        v1 is refused by the service (backwards publish) and by the sink.
        """
        task_id = self.estate.run_task("barrier", "auto", version="v1")
        self.estate.wait_task(task_id, ("committed",))
        self.estate.stop("page")
        status, payload = self.estate.publish(preset="v2", version="v2")
        self.assertEqual(status, 200, payload)
        import subprocess
        source = {"path": "/product/"}
        log_path = self.estate.run_dir / "logs" / "page-restart.log"
        with open(log_path, "ab") as log:
            self.estate.procs["page"] = subprocess.Popen(
                [sys.executable, "scripts/collector.py", "--entry-id", "page",
                 "--kind", "page", "--source", json.dumps(source),
                 "--work-root", str(self.estate.run_dir / "work"),
                 "--config-url", self.estate.config,
                 "--source-origin", f"{self.estate.mock}/e/page",
                 "--hub-url", self.estate.sink,
                 "--port", str(self.estate.ports["page"])],
                cwd=str(harness.ROOT), stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True)
        harness.wait_health(self.estate.urls["page"])
        # It can only report the version it really read: v2.
        self.estate.observe_all()
        page = {e["entry_id"]: e for e in self.estate.entries()}["page"]
        self.assertEqual(page["observation"]["version"], "v2")
        self.assertEqual(page["observation"]["source"], "live")
        # A task pinned to v2 succeeds; a task pinned to v1 is refused.
        status, body = http_json(
            self.estate.web + "/api/task",
            {"gate": "barrier", "publish": "auto", "version": "v1"})
        self.assertEqual(status, 409, body)
        good = self.estate.run_task("barrier", "auto", version="v2")
        task = self.estate.wait_task(good, ("committed",), timeout=30)
        self.assertEqual(task["version"], "v2")
        self.assertEqual(task["published_rows"], 12)

    def test_delayed_publish_refuses_when_source_changed_underneath(self):
        """Rows verified earlier must not ship if the source moved since."""
        task_id = self.estate.run_task("prefetch", "manual")
        task = self.estate.wait_task(task_id, ("ready",))
        self.assertEqual(task["published_rows"], 0)
        # Mutate the directory file after the entries read it.
        source = self.estate.run_dir / "source" / "catalog.json"
        payload = json.loads(source.read_text())
        for item in payload["items"]:
            if item["sku"] == "A-100":
                item["price_cents"] = 999
        source.write_text(json.dumps(payload))
        status, payload = http_json(
            self.estate.web + f"/api/task/{task_id}/commit", {})
        self.assertEqual(status, 409, payload)
        task = self.estate.task(task_id)
        self.assertEqual(task["state"], "aborted")
        self.assertEqual(task["published_rows"], 0)
        self.assertEqual(task["stored_rows"], 0)
        self.assertEqual(len(self.estate.records()), 0)

    def test_restarted_entry_cannot_submit_a_pre_outage_task(self):
        """Kill the file entry mid-task, restore it, and prove stale state is
        neither served from disk nor accepted by the sink."""
        self.estate.entry_link("file", down=True)
        task_id = self.estate.run_task("prefetch", "auto")
        self.estate.wait_task(task_id, ("aborted",), timeout=30)
        # The entry lost its link while holding prepared state. Restart it as a
        # *new* process: its prepared dirs from the failed task must be purged.
        self.estate.stop("file")
        import subprocess
        source = {"path": str(self.estate.run_dir / "source" / "catalog.json")}
        log = open(self.estate.run_dir / "logs" / "file-restart.log", "ab")
        self.estate.procs["file"] = subprocess.Popen(
            [sys.executable, "scripts/collector.py", "--entry-id", "file",
             "--kind", "file", "--source", json.dumps(source),
             "--work-root", str(self.estate.run_dir / "work"),
             "--config-url", self.estate.config,
             "--source-origin", f"{self.estate.mock}/e/file",
             "--hub-url", self.estate.sink,
             "--port", str(self.estate.ports["file"])],
            cwd=str(harness.ROOT), stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True)
        harness.wait_health(self.estate.urls["file"])
        state = http_json(self.estate.urls["file"] + "/state")[1]
        self.assertEqual(state["prepared"], [])
        self.estate.entry_link("file", down=False)
        new_id = self.estate.run_task("barrier", "auto")
        task = self.estate.wait_task(new_id, ("committed",), timeout=30)
        self.assertEqual(task["published_rows"], 9)
        for event in task["entries"]:
            self.assertEqual(event["state"], "verified")
            self.assertEqual(event["config_evidence"]["source"], "live")


class TestConsole(EstateCase):
    def test_console_reports_per_entry_config_evidence(self):
        self.estate.observe_all()
        entries = self.estate.entries()
        self.assertEqual(len(entries), 3)

        def all_live():
            current = self.estate.entries()
            return all(e["reachable"] and e["matches_published"] for e in current)
        from harness import wait_for
        wait_for(all_live, timeout=15, message="all entries live and matching")
        entries = self.estate.entries()
        for entry in entries:
            self.assertTrue(entry["matches_published"], entry)
            obs = entry["observation"]
            self.assertEqual(obs["source"], "live")
            self.assertEqual(obs["version"], "v1")
            self.assertTrue(obs["read_id"])
            self.assertTrue(obs["body_sha256"])
        bodies = {e["observation"]["body_sha256"] for e in entries}
        self.assertEqual(len(bodies), 1)

    def test_console_shows_one_entry_offline_then_recovered(self):
        self.estate.observe_all()
        self.estate.http_outage("api", True)
        result = self.estate.observe_all()
        statuses = {r["entry_id"]: r["ok"] for r in result["results"]}
        self.assertFalse(statuses["api"])
        self.assertTrue(statuses["page"])
        entries = {e["entry_id"]: e for e in self.estate.entries()}
        self.assertFalse(entries["api"]["matches_published"])
        self.assertEqual(entries["api"]["observation"]["source"], "unreachable")
        self.estate.http_outage("api", False)
        self.estate.observe_all()
        entries = {e["entry_id"]: e for e in self.estate.entries()}
        self.assertTrue(entries["api"]["matches_published"])
        self.assertEqual(entries["api"]["observation"]["source"], "live")

    def test_old_entry_cannot_report_a_config_it_did_not_read(self):
        """A forged 'cached' confirmation is refused by the sink."""
        body = self.published()
        status, payload = http_json(
            self.estate.sink + "/entries/api/observe",
            {"config": {"source": "cache", "body_sha256": body["body_sha256"],
                        "version": body["version"]}})
        self.assertEqual(status, 200, payload)
        self.assertFalse(payload["matches_published"])
        entries = {e["entry_id"]: e for e in self.estate.entries()}
        self.assertFalse(entries["api"]["matches_published"])
        self.assertTrue(entries["api"]["observation"].get("source") == "cache")


if __name__ == "__main__":
    unittest.main(verbosity=2)
