"""Stdlib-only JSON/provenance and bounded Docker I/O for the Unix scripts."""
import datetime as dt
import hashlib
import json
import os
import pathlib
import signal
import subprocess
import sys
import time


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def read(run):
    return json.loads((run / "report.json").read_text(encoding="utf-8"))


def save(run, report):
    text = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    (run / "report.json").write_text(text, encoding="utf-8")
    (run.parent / "report.json").write_text(text, encoding="utf-8")


def main():
    action, directory, *args = sys.argv[1:]
    run = pathlib.Path(directory)
    if action == "init":
        run.mkdir(parents=True, exist_ok=False)
        mode, run_id = args
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip())
        manifest = []
        for folder in ["cmd", "internal", "gen", "proto", "migrations", "scripts"]:
            for path in sorted(pathlib.Path(folder).rglob("*")):
                if path.is_file() and "__pycache__" not in path.parts:
                    manifest.append(path.as_posix() + " " + hashlib.sha256(path.read_bytes()).hexdigest())
        for name in ["go.mod", "go.sum", "Dockerfile", "compose.yaml"]:
            manifest.append(name + " " + hashlib.sha256(pathlib.Path(name).read_bytes()).hexdigest())
        save(run, dict(mode=mode, run_id=run_id, started_at=now(), source_commit=sha,
                       source_dirty=dirty, source_content_sha256=hashlib.sha256("\n".join(manifest).encode()).hexdigest(),
                       status="running", steps=[], measurements=[]))
        return
    report = read(run)
    if action == "config":
        # Never persist the full interpolated configuration: it contains secrets.
        result = subprocess.run(["docker", "compose", "config", "--format", "json"], capture_output=True, timeout=30)
        if result.returncode:
            raise RuntimeError("Compose configuration failed; private output omitted")
        config = json.loads(result.stdout)
        gateway = config["services"]["gateway"]["environment"]
        report["rate_limit"] = str(gateway["RATE_LIMIT"])
        report["rate_window"] = str(gateway["RATE_WINDOW"])
        report["services"] = [dict(service=name, **{field: config["services"][name].get(field)
                              for field in ["image", "cpus", "mem_limit"]})
                              for name in ["db", "kafka", "redis", "ledger", "publisher", "activity", "gateway", "gateway2"]]
        save(run, report)
        print(report["rate_limit"], report["rate_window"])
    elif action == "run":
        label, timeout, *command = args
        stdout_file = run / f"{len(report['steps'])+1:02d}-{label}.stdout.log"
        stderr_file = stdout_file.with_name(stdout_file.name.replace("stdout", "stderr"))
        started, clock = now(), time.monotonic()
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        timed_out = False
        try:
            stdout, stderr = process.communicate(timeout=int(timeout))
            code = process.returncode
        except subprocess.TimeoutExpired:
            # Only the process group created for this command is terminated.
            os.killpg(process.pid, signal.SIGKILL)
            stdout, stderr = process.communicate(timeout=5)
            code, timed_out = -1, True
        stdout_file.write_bytes(stdout)
        stderr_file.write_bytes(stderr)
        report["steps"].append(dict(name=label, started_at=started, finished_at=now(),
                                    seconds=time.monotonic()-clock, exit_code=code, timed_out=timed_out,
                                    stdout_file=str(stdout_file), stderr_file=str(stderr_file)))
        save(run, report)
        print(stdout_file)
        if code:
            print(f"Docker phase {label} failed, exit {code}; inspect {stderr_file}", file=sys.stderr)
            sys.exit(1)
    elif action in ["phase", "bench", "host"]:
        label, file = args
        value = json.loads(pathlib.Path(file).read_text(encoding="utf-8"))
        if action == "phase":
            report["measurements"].append(dict(phase=label, command_seconds=report["steps"][-1]["seconds"], result=value))
        else:
            report["benchmark" if action == "bench" else "host"] = value
        save(run, report)
    elif action == "backlog":
        file, minimum = args
        value = json.loads(pathlib.Path(file).read_text(encoding="utf-8"))
        if value["outbox_backlog"] < int(minimum):
            raise RuntimeError("Kafka outage did not retain the expected unpublished events")
    elif action == "projection-lag":
        file, minimum = args
        value = json.loads(pathlib.Path(file).read_text(encoding="utf-8"))
        if value["outbox_events"] - value["projected_events"] < int(minimum):
            raise RuntimeError("Stopped Activity did not retain the expected unprojected events")
    elif action == "shutdown":
        service, file, *details = args
        step_name = details[0] if details else "sigterm-" + service
        code = int(pathlib.Path(file).read_text().strip())
        seconds = next(step["seconds"] for step in reversed(report["steps"]) if step["name"] == step_name)
        measurement = dict(phase="sigterm", service=service, docker_stop_seconds=seconds, container_exit_code=code)
        if len(details) > 1:
            measurement["scenario"] = details[1]
        report["measurements"].append(measurement)
        save(run, report)
        if code:
            raise RuntimeError(f"{service} did not exit cleanly on SIGTERM, exit {code}")
    elif action == "outage":
        start, hold, *details = args
        component = details[0] if details else "kafka"
        report[component + "_outage"] = dict(explicit_hold_seconds=int(hold), stop_start_command_span_seconds=time.time()-int(start))
        save(run, report)
    elif action == "recovery":
        service, reason, step_name = args
        seconds = next(step["seconds"] for step in reversed(report["steps"]) if step["name"] == step_name)
        report["measurements"].append(dict(phase="recovery", service=service, reason=reason,
                                           action="explicit_operator_restart", command_seconds=seconds))
        save(run, report)
    elif action == "finish":
        status, error, *details = args
        report.update(status=status, finished_at=now())
        if details:
            report["services_restored"] = details[0] == "1"
        if error:
            report["error"] = error
        save(run, report)
    else:
        raise ValueError("unknown report action")


if __name__ == "__main__":
    main()
