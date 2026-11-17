"""Shared test harness: start a complete estate with ephemeral ports."""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http_json(url: str, body=None, method: str | None = None, timeout: float = 10.0):
    data = None
    headers = {"Content-Type": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers,
                                 method=method or ("POST" if data else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw)
        except ValueError:
            return exc.code, {"raw": raw.decode("utf-8", "replace")}


def wait_for(predicate, timeout: float = 20.0, interval: float = 0.1,
             message: str = "condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {message}; last={last!r}")


def wait_health(url: str, timeout: float = 20.0) -> None:
    def probe():
        try:
            status, _ = http_json(url + "/health", timeout=0.5)
            return status == 200
        except OSError:
            return False
    wait_for(probe, timeout, message=f"health at {url}")


class Estate:
    """All seven processes on ephemeral ports under a temp run dir."""

    def __init__(self, run_dir: Path, preset: str = "v1"):
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / "logs").mkdir(exist_ok=True)
        self.procs: dict[str, subprocess.Popen] = {}
        self.ports = {name: free_port() for name in
                      ("mock", "config", "api", "file", "page", "sink", "web")}
        self.urls: dict[str, str] = {}
        self.preset = preset

    # -------------------------------------------------------------- process
    def start(self, name: str, argv: list[str], health: str | None = None) -> None:
        log_path = self.run_dir / "logs" / f"{name}.log"
        with open(log_path, "ab") as log:
            self.procs[name] = subprocess.Popen(
                [sys.executable, *argv], cwd=str(ROOT), stdout=log,
                stderr=subprocess.STDOUT, start_new_session=True)
        if health:
            wait_health(health)

    def stop(self, name: str, sig=None) -> None:
        import signal as _signal
        proc = self.procs.get(name)
        if proc is None:
            return
        proc.send_signal(sig or _signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        self.procs.pop(name, None)

    @property
    def mock(self) -> str:
        return self.urls["mock"]

    @property
    def config(self) -> str:
        return self.urls["config"]

    @property
    def sink(self) -> str:
        return self.urls["sink"]

    @property
    def web(self) -> str:
        return self.urls["web"]

    # ------------------------------------------------------------------ boot
    def boot(self) -> "Estate":
        py = lambda *a: [str(x) for x in a]  # noqa: E731
        self.start("mock", ["scripts/mock_estate.py", "--run-dir",
                            str(self.run_dir), "--port", str(self.ports["mock"])],
                   None)
        self.urls["mock"] = f"http://127.0.0.1:{self.ports['mock']}"
        wait_health(self.mock)

        from collect.presets import PRESETS
        seed = self.run_dir / "seed.json"
        seed.write_text(json.dumps(PRESETS[self.preset]), encoding="utf-8")
        self.start("config", ["scripts/config_service.py", "--db",
                              str(self.run_dir / "config.db"), "--port",
                              str(self.ports["config"]), "--seed", str(seed),
                              "--seed-version", self.preset], None)
        self.urls["config"] = f"http://127.0.0.1:{self.ports['config']}"
        wait_health(self.config)

        self.urls["sink"] = f"http://127.0.0.1:{self.ports['sink']}"
        source_file = str(self.run_dir / "source" / "catalog.json")
        entries = []
        for kind in ("api", "file", "page"):
            source = {"path": {"api": "/catalog", "file": source_file,
                               "page": "/product/"}[kind]}
            origin = f"{self.mock}/e/{kind}"
            url = f"http://127.0.0.1:{self.ports[kind]}"
            self.start(kind, ["scripts/collector.py", "--entry-id", kind,
                              "--kind", kind, "--source", json.dumps(source),
                              "--work-root", str(self.run_dir / "work"),
                              "--config-url", self.config,
                              "--source-origin", origin,
                              "--hub-url", self.urls["sink"],
                              "--port", str(self.ports[kind])], None)
            self.urls[kind] = url
            wait_health(url)
            entries.append({"entry_id": kind, "kind": kind,
                            "collector_url": url,
                            "mock_prefix": f"/e/{kind}", "source": source})
        spec = self.run_dir / "entries.json"
        spec.write_text(json.dumps({"entries": entries}), encoding="utf-8")
        self.start("sink", ["scripts/sink.py", "--db",
                            str(self.run_dir / "sink.db"), "--run-dir",
                            str(self.run_dir), "--config-url", self.config,
                            "--mock-url", self.mock, "--port",
                            str(self.ports["sink"]), "--entries", str(spec)],
                   None)
        wait_health(self.sink)
        self.start("web", ["web/server.py", "--sink-url", self.sink,
                           "--config-url", self.config, "--mock-url", self.mock,
                           "--port", str(self.ports["web"])], None)
        self.urls["web"] = f"http://127.0.0.1:{self.ports['web']}"
        wait_health(self.web)
        return self

    def shutdown(self) -> None:
        for name in reversed(list(self.procs)):
            self.stop(name)

    # --------------------------------------------------------------- actions
    def publish(self, preset: str | None = None, mapping=None, scope=None,
                version=None) -> tuple[int, dict]:
        body = {}
        if preset:
            body["preset"] = preset
        if mapping is not None:
            body["mapping"] = mapping
        if scope is not None:
            body["scope"] = scope
        if version:
            body["version"] = version
        return http_json(self.web + "/api/config/publish", body)

    def observe_all(self) -> dict:
        return http_json(self.web + "/api/observe", {})[1]

    def run_task(self, gate: str = "barrier", publish: str = "auto",
                 version: str | None = None) -> str:
        body = {"gate": gate, "publish": publish}
        if version:
            body["version"] = version
        status, payload = http_json(self.web + "/api/task", body)
        assert status in (200, 201), (status, payload)
        return payload["task_id"]

    def task(self, task_id: str) -> dict:
        return http_json(self.sink + f"/tasks/{task_id}")[1]

    def wait_task(self, task_id: str, states: tuple[str, ...],
                  timeout: float = 25.0) -> dict:
        return wait_for(lambda: (lambda t: t if t["state"] in states else None)(
            self.task(task_id)), timeout, message=f"task {task_id} in {states}")

    def outage(self, entry: str, down: bool) -> dict:
        return http_json(self.mock + "/admin/outage",
                         {"entry": entry, "down": down})[1]

    def set_outage(self, entry: str, down: bool) -> dict:
        """Provider-side outage: the mock drops this entry's prefix."""
        return http_json(self.mock + "/admin/outage",
                         {"entry": entry, "down": down})[1]

    def entry_link(self, entry: str, down: bool) -> dict:
        """Entry-side link control: the collector refuses config+source reads."""
        return http_json(self.urls[entry] + "/admin/link",
                         {"up": not down, "reason": "test"})[1]

    def http_outage(self, entry: str, down: bool) -> dict:
        """Same switch the console's buttons use."""
        return http_json(self.web + "/api/outage",
                         {"entry": entry, "down": down})[1]

    def entries(self) -> list[dict]:
        return http_json(self.sink + "/entries")[1]["entries"]

    def records(self, task_id: str | None = None) -> list[dict]:
        url = self.sink + "/records" + (f"?task={task_id}" if task_id else "")
        return http_json(url)[1]["records"]

    def events(self) -> list[dict]:
        return http_json(self.sink + "/events")[1]["events"]
