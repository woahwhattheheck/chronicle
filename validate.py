"""One source-pinned native Windows comparison; never advances the bounty branch."""
from __future__ import annotations
import hashlib
import json
import os
import pathlib
import platform
import re
import shutil
import subprocess
import sys
import time

BASE = "cf57d7c658acfb91f23b4e1ccc66794187607ce0"
SOURCE = pathlib.Path(sys.argv[1]).resolve()
CONTROL = pathlib.Path(__file__).resolve().parent
EVIDENCE = pathlib.Path(sys.argv[2]).resolve()
EVIDENCE.mkdir(parents=True, exist_ok=True)
ORIGINAL = {
    "internal/raft/core.go": "db1c289be88acd496ab20834e8de871cadcfb0d6",
    "internal/raft/log.go": "c375931004dc03beedf19b7831f962ccc651c911",
    "internal/raft/core_test.go": "c0c1542f9b3e69c2deec5a96fb3ac8772056c0fe",
    "internal/raft/hardening_test.go": "ea6d7acc68d7db6e1b91fff46cf078137db71cd4",
    "internal/raft/raft_hardening_test.go": "b60a8fe278756f7134c10cabd670db9f3bdee463",
}
NEW_TEST = "internal/raft/cleanup_test.go"
TARGETS = ["TestRaftPersistentState", "TestRaftSnapshot", "TestLogCompactor_Compact", "TestLogCompactor_NewInstance", "TestMaybeSnapshot_BelowThreshold", "TestMaybeSnapshot_AboveThreshold", "TestConfirmLeadership_ExpiredLease", "TestRaftNodeStartStop", "TestRaftResourceLifecycle", "TestRaftConstructorFailureReleasesLog", "TestRaftLogConstructorFailureReleasesFile"]
COMMAND = ["go", "test", "-short", "-count=1", "-p=2", "-timeout=120s", "-json", "-run", "^(" + "|".join(TARGETS) + ")$", "./internal/raft"]

def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=SOURCE, text=True).strip()

def replace(path: str, old: str, new: str, count: int = 1) -> None:
    p = SOURCE / path
    text = p.read_text(encoding="utf-8")
    assert text.count(old) == count, (path, "preimage count", text.count(old), count)
    p.write_bytes(text.replace(old, new).encode())

def add_cleanup(function: str, variable: str) -> None:
    path = "internal/raft/core_test.go"
    p = SOURCE / path
    text = p.read_text(encoding="utf-8")
    start = text.index("func " + function + "(")
    end = text.find("\nfunc ", start + 1)
    if end < 0:
        end = len(text)
    body = text[start:end]
    pattern = re.escape("\t" + variable + ", err := NewRaftNode(store, config)\n") + r"\tif err != nil \{\n[^\n]+\n\t\}\n"
    body, n = re.subn(pattern, lambda m: m.group() + "\tstopRaftNodeOnCleanup(t, " + variable + ")\n", body)
    assert n == 1, (function, variable, n)
    p.write_bytes((text[:start] + body + text[end:]).encode())

def fingerprint(paths: list[str]) -> dict[str, str]:
    return {p: hashlib.sha256((SOURCE / p).read_bytes()).hexdigest() for p in paths}

def run(label: str) -> dict:
    started = time.monotonic()
    with (EVIDENCE / (label + ".jsonl")).open("wb") as out, (EVIDENCE / (label + ".stderr")).open("wb") as err:
        result = subprocess.run(COMMAND, cwd=SOURCE, stdout=out, stderr=err, timeout=420)
    events = [json.loads(line) for line in (EVIDENCE / (label + ".jsonl")).read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    summary = {"command": COMMAND, "exit": result.returncode, "wall_seconds": time.monotonic() - started, "top_level_runs": [e["Test"] for e in events if e.get("Action") == "run" and "/" not in e.get("Test", "")], "failed_tests": [e["Test"] for e in events if e.get("Action") == "fail" and e.get("Test")], "passed_tests": [e["Test"] for e in events if e.get("Action") == "pass" and e.get("Test")], "package_events": [e for e in events if not e.get("Test") and e.get("Action") in ("pass", "fail")]}
    (EVIDENCE / (label + "-summary.json")).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)
    assert set(summary["top_level_runs"]) == set(TARGETS), "selected tests did not all execute"
    return summary

assert platform.system() == "Windows", platform.platform()
assert git("rev-parse", "HEAD") == BASE
assert not git("status", "--porcelain"), "source checkout must start clean"
for path, sha in ORIGINAL.items():
    assert git("rev-parse", "HEAD:" + path) == sha, path
    assert (SOURCE / path).read_bytes() == subprocess.check_output(["git", "show", "HEAD:" + path], cwd=SOURCE), path
assert not (SOURCE / NEW_TEST).exists()
assert "stopRaftNodeOnCleanup" not in "\n".join(p.read_text(encoding="utf-8") for p in (SOURCE / "internal/raft").glob("*.go"))
provenance = {"base": BASE, "base_tree": git("rev-parse", "HEAD^{tree}"), "original_blobs": ORIGINAL, "platform": platform.platform(), "go": subprocess.check_output(["go", "version"], text=True).strip(), "cgo_enabled": os.environ.get("CGO_ENABLED"), "run_id": os.environ.get("GITHUB_RUN_ID"), "controller": os.environ.get("GITHUB_SHA"), "baseline_note": "Original production with identical explicit test-fixture cleanup and new resource regressions in both executions."}
(EVIDENCE / "provenance.json").write_text(json.dumps(provenance, indent=2), encoding="utf-8")

# Both executions use the same test bodies and checked cleanup registrations.
shutil.copyfile(CONTROL / "cleanup_test.go", SOURCE / NEW_TEST)
add_cleanup("TestRaftPersistentState", "node1")
add_cleanup("TestRaftPersistentState", "node2")
add_cleanup("TestRaftSnapshot", "node")
old = '\tnode, err := NewRaftNode(store, config)\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n'
replace("internal/raft/raft_hardening_test.go", old, old + "\tstopRaftNodeOnCleanup(t, node)\n", 4)
old = '\traftLog, err := NewRaftLog(dir + "/wal")\n\tif err != nil {\n\t\tt.Fatalf("NewRaftLog: %v", err)\n\t}\n'
replace("internal/raft/hardening_test.go", old, old + '\tt.Cleanup(func() {\n\t\tif err := raftLog.Close(); err != nil {\n\t\t\tt.Errorf("Close: %v", err)\n\t\t}\n\t})\n')
test_paths = [p for p in ORIGINAL if p.endswith("_test.go")] + [NEW_TEST]
subprocess.run(["gofmt", "-w", *test_paths], cwd=SOURCE, check=True)
test_hashes = fingerprint(test_paths)
before = run("before")
assert before["exit"] == 1 and "TestRaftResourceLifecycle/unstarted" in before["failed_tests"], "baseline did not reproduce the unstarted-node leak"
assert "TestRaftConstructorFailureReleasesLog" in before["failed_tests"]
assert "TestRaftLogConstructorFailureReleasesFile" in before["failed_tests"]

# Product fixes: serialize lifecycle, close owned files on every terminal path.
replace("internal/raft/core.go", '\tlisteners []RaftEventListener\n}', '\tlisteners []RaftEventListener\n\n\tlifecycleMu sync.Mutex\n\tstopped     bool\n\tstopErr     error\n}')
replace("internal/raft/core.go", '\tif err := rn.loadState(); err != nil {\n\t\tcancel()\n', '\tif err := rn.loadState(); err != nil {\n\t\t_ = log.Close()\n\t\tcancel()\n')
replace("internal/raft/core.go", 'func (rn *RaftNode) Start() error {\n', 'func (rn *RaftNode) Start() error {\n\trn.lifecycleMu.Lock()\n\tdefer rn.lifecycleMu.Unlock()\n\tif rn.stopped {\n\t\treturn errors.New("raft node has been stopped")\n\t}\n')
replace("internal/raft/core.go", '// Stop stops Raft operations.\nfunc (rn *RaftNode) Stop() error {\n\tif !rn.running.Load() {\n\t\treturn nil\n\t}\n', '// Stop releases node resources, including nodes that were never started.\n// A stopped node cannot be restarted; repeated calls return the same result.\nfunc (rn *RaftNode) Stop() error {\n\trn.lifecycleMu.Lock()\n\tdefer rn.lifecycleMu.Unlock()\n\tif rn.stopped {\n\t\treturn rn.stopErr\n\t}\n\trn.stopped = true\n\tif !rn.running.Load() {\n\t\trn.cancel()\n\t\trn.stopErr = rn.log.Close()\n\t\treturn rn.stopErr\n\t}\n')
replace("internal/raft/core.go", '\tif err := rn.log.Close(); err != nil {\n\t\treturn err\n\t}\n\n\treturn rn.saveState()\n', '\tif err := rn.log.Close(); err != nil {\n\t\trn.stopErr = err\n\t\treturn err\n\t}\n\n\trn.stopErr = rn.saveState()\n\treturn rn.stopErr\n')
replace("internal/raft/log.go", '\t\tif err := rl.load(); err != nil {\n\t\t\treturn nil, err\n', '\t\tif err := rl.load(); err != nil {\n\t\t\t_ = file.Close()\n\t\t\treturn nil, err\n')
subprocess.run(["gofmt", "-w", "internal/raft/core.go", "internal/raft/log.go"], cwd=SOURCE, check=True)
assert fingerprint(test_paths) == test_hashes, "test bodies changed between executions"
subprocess.run(["git", "diff", "--check"], cwd=SOURCE, check=True)
after = run("after")
assert after["exit"] == 0 and not after["failed_tests"], "candidate acceptance failed"
changed = sorted([*ORIGINAL, NEW_TEST])
subprocess.run(["git", "add", "--", *changed], cwd=SOURCE, check=True)
assert git("diff", "--cached", "--name-only").splitlines() == changed
(EVIDENCE / "repair.diff").write_bytes(subprocess.check_output(["git", "diff", "--cached", "--no-ext-diff"], cwd=SOURCE))
subprocess.run(["git", "config", "user.name", "woahwhattheheck"], cwd=SOURCE, check=True)
subprocess.run(["git", "config", "user.email", "woahwhattheheck@users.noreply.github.com"], cwd=SOURCE, check=True)
subprocess.run(["git", "commit", "-m", "fix: release Raft resources before start and on construction failure [skip ci]"], cwd=SOURCE, check=True)
candidate = git("rev-parse", "HEAD")
assert git("rev-parse", "HEAD^") == BASE
receipt = {**provenance, "candidate": candidate, "candidate_tree": git("rev-parse", "HEAD^{tree}"), "candidate_blobs": {p: git("rev-parse", "HEAD:" + p) for p in changed}, "test_sha256_before_and_after": test_hashes, "before": before, "after": after, "limitations": ["Selected native Windows resource-lifecycle tests only", "No full-suite, race-detector, multi-node load, award or payment claim"]}
(EVIDENCE / "receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
for path in changed:
    target = EVIDENCE / "candidate" / path
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(SOURCE / path, target)
subprocess.run(["git", "push", "origin", "HEAD:refs/heads/validation/chronicle104-raft-stop-result-relay5692"], cwd=SOURCE, check=True)
print("RETAINED CODE-ONLY CANDIDATE " + candidate, flush=True)
