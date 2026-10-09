"""Single, Docker-free entry point for the complete CDC stack."""

from __future__ import annotations

import argparse
import hashlib
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent
KAFKA_HOME = ROOT / "vendor" / "kafka" / "kafka_2.13-4.1.2"
PLUGIN_HOME = ROOT / "vendor" / "plugins" / "debezium-mysql" / "debezium-connector-mysql"
WHEELS = ROOT / "vendor" / "python-wheels"
RUNTIME = ROOT / "runtime"
LOGS = RUNTIME / "logs"
PIDS = RUNTIME / "pids"
VENV = ROOT / ".venv"
SERVER_CONFIG = ROOT / "config" / "kafka" / "server.properties"
CONNECT_CONFIG = ROOT / "config" / "kafka" / "connect-distributed.properties"
CLUSTER_ID = "MkU3OEVBNTcwNTJENDM2Qk"


def load_dotenv() -> None:
    path = ROOT / ".env"
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(name.strip(), value)


def java_home() -> Path:
    matches = sorted((ROOT / "vendor" / "java").glob("*/bin/java.exe"))
    if not matches:
        matches = sorted((ROOT / "vendor" / "java").glob("*/bin/java"))
    if not matches:
        raise FileNotFoundError("Bundled Java runtime is missing under vendor/java")
    return matches[0].parent.parent


def venv_python() -> Path:
    return VENV / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def environment() -> dict[str, str]:
    env = os.environ.copy()
    bundled_java = java_home()
    env.update({
        "JAVA_HOME": str(bundled_java),
        "PATH": str(bundled_java / "bin") + os.pathsep + env.get("PATH", ""),
        "CDC_DATABASES_CONFIG": str(ROOT / "config" / "databases.yml"),
        "CDC_TABLES_CONFIG": str(ROOT / "config" / "tables.yml"),
        "CDC_S3_CONFIG": str(ROOT / "config" / "s3.yml"),
        "CDC_KAFKA_BOOTSTRAP": "127.0.0.1:9092",
        "CDC_CONNECT_URL": "http://127.0.0.1:8083",
    })
    return env


def ensure_assets() -> None:
    required = [
        KAFKA_HOME / "libs",
        SERVER_CONFIG,
        CONNECT_CONFIG,
    ]
    missing = [str(path.relative_to(ROOT)) for path in required if not path.exists()]
    if not list(PLUGIN_HOME.glob("debezium-connector-mysql-*.jar")):
        missing.append(str(PLUGIN_HOME.relative_to(ROOT) / "debezium-connector-mysql-*.jar"))
    if not WHEELS.exists() or not list(WHEELS.glob("*.whl")):
        missing.append(str(WHEELS.relative_to(ROOT) / "*.whl"))
    java_home()
    if missing:
        raise FileNotFoundError("Offline assets are missing: " + ", ".join(missing))


def ensure_venv() -> None:
    ensure_assets()
    digest = hashlib.sha256((ROOT / "requirements.txt").read_bytes()).hexdigest()
    marker = VENV / ".offline-requirements.sha256"
    if venv_python().exists() and marker.exists() and marker.read_text().strip() == digest:
        return
    if not venv_python().exists():
        print("Creating local Python virtual environment...", flush=True)
        subprocess.run([sys.executable, "-m", "venv", str(VENV)], cwd=ROOT, check=True)
    print("Installing Python modules from the offline wheel cache...", flush=True)
    subprocess.run([
        str(venv_python()), "-m", "pip", "install", "--no-index",
        "--find-links", str(WHEELS), "--requirement", str(ROOT / "requirements.txt"),
    ], cwd=ROOT, env=environment(), check=True)
    marker.write_text(digest + "\n", encoding="utf-8")


def python_command(*arguments: str, check: bool = True) -> subprocess.CompletedProcess:
    ensure_venv()
    command = [str(venv_python()), *arguments]
    print("+", " ".join(command), flush=True)
    return subprocess.run(command, cwd=ROOT, env=environment(), check=check)


def kafka_command(component: str, *arguments: str) -> list[str]:
    """Launch Kafka directly, avoiding Windows batch classpath length limits."""
    definitions = {
        "storage": ("kafka.tools.StorageTool", "-Xms128m", "-Xmx512m", "tool-log4j2.yaml"),
        "server": ("kafka.Kafka", "-Xms512m", "-Xmx1g", "log4j2.yaml"),
        "connect": (
            "org.apache.kafka.connect.cli.ConnectDistributed",
            "-Xms256m", "-Xmx768m", "connect-log4j2.yaml",
        ),
    }
    main_class, minimum_heap, maximum_heap, logging_config = definitions[component]
    java = java_home() / "bin" / ("java.exe" if os.name == "nt" else "java")
    return [
        str(java), minimum_heap, maximum_heap,
        f"-Dlog4j2.configurationFile={(KAFKA_HOME / 'config' / logging_config).as_uri()}",
        f"-Dkafka.logs.dir={LOGS}",
        "-cp", str(KAFKA_HOME / "libs" / "*"), main_class, *arguments,
    ]


def is_running(name: str) -> bool:
    path = PIDS / f"{name}.pid"
    if not path.exists():
        return False
    try:
        pid = int(path.read_text().strip())
        if os.name == "nt":
            import ctypes
            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
            if not handle:
                path.unlink(missing_ok=True)
                return False
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        path.unlink(missing_ok=True)
        return False


def launch(name: str, command: list[str]) -> None:
    if is_running(name):
        print(f"{name} is already running.")
        return
    LOGS.mkdir(parents=True, exist_ok=True)
    PIDS.mkdir(parents=True, exist_ok=True)
    log_path = LOGS / f"{name}.log"
    env = environment()
    flags = 0
    kwargs = {}
    if os.name == "nt":
        flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    else:
        kwargs["start_new_session"] = True
    with log_path.open("ab", buffering=0) as output:
        process = subprocess.Popen(
            command, cwd=ROOT, env=env, stdin=subprocess.DEVNULL,
            stdout=output, stderr=subprocess.STDOUT, creationflags=flags, **kwargs,
        )
    (PIDS / f"{name}.pid").write_text(str(process.pid) + "\n", encoding="utf-8")
    print(f"Started {name} (PID {process.pid}); log: {log_path.relative_to(ROOT)}")


def wait_for_port(port: int, timeout: int, service: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(1)
    raise TimeoutError(f"{service} did not open port {port} within {timeout} seconds; check runtime/logs/{service}.log")


def wait_for_connect(timeout: int = 180) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen("http://127.0.0.1:8083/connectors", timeout=2) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.HTTPError):
            time.sleep(1)
    raise TimeoutError("Kafka Connect REST API was not ready; check runtime/logs/connect.log")


def format_kafka() -> None:
    metadata = RUNTIME / "kafka-data" / "meta.properties"
    if metadata.exists():
        return
    LOGS.mkdir(parents=True, exist_ok=True)
    (RUNTIME / "kafka-data").mkdir(parents=True, exist_ok=True)
    command = kafka_command(
        "storage", "format", "--standalone", "--cluster-id", CLUSTER_ID,
        "--config", str(SERVER_CONFIG),
    )
    print("Formatting the local KRaft data directory...", flush=True)
    subprocess.run(command, cwd=ROOT, env=environment(), check=True)


def stop_process(name: str) -> None:
    path = PIDS / f"{name}.pid"
    if not is_running(name):
        print(f"{name} is not running.")
        return
    pid = int(path.read_text().strip())
    if os.name == "nt":
        result = subprocess.run(["taskkill", "/PID", str(pid), "/T"], capture_output=True)
        if result.returncode != 0:
            subprocess.run(["taskkill", "/F", "/PID", str(pid), "/T"], check=False)
    else:
        os.killpg(pid, signal.SIGTERM)
    for _ in range(20):
        if not is_running(name):
            break
        time.sleep(0.5)
    if is_running(name) and os.name == "nt":
        subprocess.run(["taskkill", "/F", "/PID", str(pid), "/T"], check=False)
    path.unlink(missing_ok=True)
    print(f"Stopped {name}.")


def start() -> None:
    ensure_venv()
    python_command("-m", "cdc.cli", "validate")
    format_kafka()
    launch("kafka", kafka_command("server", str(SERVER_CONFIG)))
    wait_for_port(9092, 120, "kafka")
    launch("connect", kafka_command("connect", str(CONNECT_CONFIG)))
    wait_for_connect()
    python_command("-m", "cdc.cli", "apply", "--wait")
    launch("sink", [str(venv_python()), "-m", "cdc.sink"])
    print("CDC stack is running without Docker. Use `python manage.py status` to inspect it.")


def status() -> None:
    for name in ("kafka", "connect", "sink"):
        print(f"{name}: {'RUNNING' if is_running(name) else 'STOPPED'}")
    if is_running("connect"):
        python_command("-m", "cdc.cli", "status", check=False)


def verify_bundle() -> None:
    ensure_assets()
    archive = ROOT / "vendor" / "archives" / "kafka_2.13-4.1.2.tgz"
    published = (ROOT / "vendor" / "archives" / "kafka_2.13-4.1.2.tgz.sha512").read_text()
    expected = "".join(published.split(":", 1)[1].split()).upper()
    actual = hashlib.sha512(archive.read_bytes()).hexdigest().upper()
    if actual != expected:
        raise ValueError("Kafka archive SHA-512 does not match its published checksum")
    print("Offline bundle is present; Kafka SHA-512 is valid.")


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Manage the native MySQL CDC to S3 stack.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("setup", help="Create the Python environment entirely from local wheels.")
    subparsers.add_parser("start", help="Start Kafka, Connect/Debezium, and the S3 sink.")
    subparsers.add_parser("stop", help="Stop all native services without deleting offsets/data.")
    subparsers.add_parser("restart", help="Stop and start all native services.")
    subparsers.add_parser("status", help="Show native process and connector state.")
    subparsers.add_parser("discover", help="Append newly discovered tables.")
    subparsers.add_parser("sync", help="Validate and apply Debezium connectors.")
    subparsers.add_parser("validate", help="Validate all YAML configuration.")
    subparsers.add_parser("verify-bundle", help="Check required offline files and Kafka checksum.")
    args = parser.parse_args()
    try:
        if args.command == "setup":
            ensure_venv()
            print("Offline Python environment is ready.")
        elif args.command == "start":
            start()
        elif args.command == "stop":
            for service in ("sink", "connect", "kafka"):
                stop_process(service)
        elif args.command == "restart":
            for service in ("sink", "connect", "kafka"):
                stop_process(service)
            start()
        elif args.command == "status":
            status()
        elif args.command == "discover":
            python_command("-m", "cdc.cli", "discover")
        elif args.command == "sync":
            python_command("-m", "cdc.cli", "validate")
            python_command("-m", "cdc.cli", "apply", "--wait")
        elif args.command == "validate":
            python_command("-m", "cdc.cli", "validate")
        elif args.command == "verify-bundle":
            verify_bundle()
    except (FileNotFoundError, OSError, RuntimeError, ValueError, subprocess.CalledProcessError, TimeoutError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()

