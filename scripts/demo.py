#!/usr/bin/env python3
"""Bring up the whole local estate, or a subset, for manual exploration.

    scripts/demo.py up      [--run-dir .run]        everything
    scripts/demo.py status  [--run-dir .run]
    scripts/demo.py down    [--run-dir .run]

The web console starts at http://127.0.0.1:8088/ (configurable).

Processes started, in dependency order:

    mock estate -> config service -> three collectors -> sink -> web
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
RUN_DEFAULT = ROOT / ".run"
PORTS = {"mock": 18080, "config": 18081, "api": 18082, "file": 18083,
         "page": 18084, "sink": 18085, "web": 18088}
SOURCES = {
    "api": {"path": "/catalog"},
    "file": {"path": "__SOURCE_FILE__"},
    "page": {"path": "/product/", "ids": []},
}


def wait_health(url: str, timeout: float = 20.0) -> None:
    import urllib.error
    import urllib.request
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url + "/health", timeout=0.5) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, OSError, ValueError):
            pass
        time.sleep(0.1)
    raise SystemExit(f"service did not become healthy: {url}")


def spawn(name: str, argv: list[str], run_dir: Path) -> subprocess.Popen:
    log = open(run_dir / "logs" / f"{name}.log", "ab")
    proc = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT,
                            cwd=str(ROOT), start_new_session=True)
    (run_dir / "pids").mkdir(exist_ok=True)
    (run_dir / "pids" / f"{name}.pid").write_text(str(proc.pid))
    return proc


def up(run_dir: Path, web_port: int) -> dict:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(exist_ok=True)
    (run_dir / "pids").mkdir(exist_ok=True)
    py = sys.executable
    info: dict = {"run_dir": str(run_dir), "urls": {}}

    mock = spawn("mock", [py, "scripts/mock_estate.py", "--run-dir",
                          str(run_dir), "--port", str(PORTS["mock"])], run_dir)
    mock_url = f"http://127.0.0.1:{PORTS['mock']}"
    wait_health(mock_url)
    info["urls"]["mock"] = mock_url

    seed = run_dir / "seed.json"
    from collect.presets import PRESETS
    seed.write_text(json.dumps(PRESETS["v1"], indent=2), encoding="utf-8")
    config = spawn("config", [py, "scripts/config_service.py", "--db",
                              str(run_dir / "config.db"), "--port",
                              str(PORTS["config"]), "--seed", str(seed),
                              "--seed-version", "v1"], run_dir)
    config_url = f"http://127.0.0.1:{PORTS['config']}"
    wait_health(config_url)
    info["urls"]["config"] = config_url

    source_file = str(run_dir / "source" / "catalog.json")
    entries = []
    for kind in ("api", "file", "page"):
        port = PORTS[kind]
        source = dict(SOURCES[kind])
        if kind == "file":
            source["path"] = source_file
        origin = f"{mock_url}/e/{kind}"
        proc = spawn(kind, [py, "scripts/collector.py", "--entry-id", kind,
                            "--kind", kind, "--source", json.dumps(source),
                            "--work-root", str(run_dir / "work"),
                            "--config-url", config_url,
                            "--source-origin", origin,
                            "--hub-url", f"http://127.0.0.1:{PORTS['sink']}",
                            "--port", str(port)], run_dir)
        url = f"http://127.0.0.1:{port}"
        wait_health(url)
        info["urls"][kind] = url
        entries.append({"entry_id": kind, "kind": kind, "collector_url": url,
                        "mock_prefix": f"/e/{kind}", "source": source})

    spec = run_dir / "entries.json"
    spec.write_text(json.dumps({"entries": entries}, indent=2), encoding="utf-8")
    sink = spawn("sink", [py, "scripts/sink.py", "--db",
                          str(run_dir / "sink.db"), "--run-dir", str(run_dir),
                          "--config-url", config_url, "--mock-url", mock_url,
                          "--port", str(PORTS["sink"]), "--entries", str(spec)],
                 run_dir)
    sink_url = f"http://127.0.0.1:{PORTS['sink']}"
    wait_health(sink_url)
    info["urls"]["sink"] = sink_url

    web = spawn("web", [py, "web/server.py", "--run-dir", str(run_dir),
                        "--sink-url", sink_url, "--config-url", config_url,
                        "--mock-url", mock_url, "--port", str(web_port)], run_dir)
    web_url = f"http://127.0.0.1:{web_port}"
    wait_health(web_url)
    info["urls"]["web"] = web_url

    (run_dir / "estate.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
    return info


def load_info(run_dir: Path) -> dict:
    path = run_dir / "estate.json"
    if not path.exists():
        raise SystemExit(f"no estate running under {run_dir}")
    return json.loads(path.read_text(encoding="utf-8"))


def down(run_dir: Path) -> None:
    pids = run_dir / "pids"
    names = []
    if pids.exists():
        names = sorted(p.name[:-4] for p in pids.glob("*.pid"))
    # Reverse start order.
    for name in reversed(names):
        pid_file = pids / f"{name}.pid"
        try:
            pid = int(pid_file.read_text())
        except (OSError, ValueError):
            continue
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        pid_file.unlink(missing_ok=True)
    deadline = time.time() + 10
    while time.time() < deadline and any(_alive(pids / f"{n}.pid") for n in names):
        # pid files are removed above; just wait for the sockets to die
        time.sleep(0.1)
        break
    (run_dir / "estate.json").unlink(missing_ok=True)


def _alive(path: Path) -> bool:
    try:
        pid = int(path.read_text())
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return False


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["up", "down", "status"])
    ap.add_argument("--run-dir", default=str(RUN_DEFAULT))
    ap.add_argument("--web-port", type=int, default=PORTS["web"])
    args = ap.parse_args(argv)
    run_dir = Path(args.run_dir).resolve()
    if args.command == "up":
        print(json.dumps(up(run_dir, args.web_port), indent=2))
    elif args.command == "down":
        down(run_dir)
        print("stopped")
    else:
        print(json.dumps(load_info(run_dir), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
