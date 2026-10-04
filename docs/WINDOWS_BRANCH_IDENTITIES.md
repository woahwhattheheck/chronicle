# Windows branch and commit identities

## Production repair

The timestamp-only branch and commit allocators both repeated IDs in a native Windows run. New branches could share a parent's key, and repeated commit keys could overwrite history. A process-wide atomic sequence now disambiguates both allocators while retaining their existing prefixes and timestamp component. The registries are process-local; this does not add persistent or distributed identity guarantees. No dependencies, merge policies, point storage, or lock protocols changed.

## Recorded native execution: October 4, 2026

[Run 37205348415](https://github.com/woahwhattheheck/chronicle/actions/runs/37205348415) used Windows Server 2025 Datacenter, 10.0.26100, amd64, Go 1.24.13 and `CGO_ENABLED=0`. The original source plus the new regression failed both allocator checks and the history group. After the allocator repair, four selected top-level groups passed: identity uniqueness, commit history, the maintained branch diff, and the maintained manual merge conflict. The two allocator subcases also passed. No clock was mocked.

**The five-group command exited 1**, because `TestTSDiffMerge_DiffBranches` rejected a zero measured duration with `expected positive diff duration`. That is a separate assertion in the other diff engine, not an identity failure. Its result is retained; neither the assertion nor production timing was changed, and there is no five-group or repository-wide pass claim.

The identity group uses 8 concurrent workers and 1,024 IDs each for each allocator. The history group checks distinct branches and 32 sequential commit identities and parent links using only in-memory manager operations. The two maintained passing groups retain their real database setup.

## Source and publication

The first controller stopped before pushing its local candidate `a3d7fbc` because the broader selection contained the duration failure. Publication reconstructed the same immutable controller recipe against its pinned source and blob, using Go 1.24.13 formatting, and retained the mixed results without repeating tests. The full source hashes below identify the published reconstruction; the original job did not emit post-repair blob hashes before its stop. The original recipe, source preimage, complete JSON output, and separate publication commit make that distinction explicit.

The original `windows-branch-identity-evidence` artifact is retained for 14 days. The publication artifact also retains its raw logs and the reconstructed source files. These results do not establish upstream CI approval, cgo or race-detector execution, bounty acceptance, or payment.

## Focused reproduction

```powershell
$env:CGO_ENABLED = '0'
go test -json -count=1 -short -p=2 -timeout=4m -run '^(TestTSBranchIdentityUniqueness|TestTSBranchHistoryIdentities|TestBranchDiff|TestManualMergeConflict)$' .
```

The command above selects the four groups that passed in the recorded broader invocation; it is provided for reproduction and was not run separately.

## Recorded results and source pins

```json
{
  "recorded_run": "https://github.com/woahwhattheheck/chronicle/actions/runs/37205348415",
  "recorded_controller": "8ae0a1fc68721de5ab2eefbdb734f63e90f8baaf",
  "recorded_base": "8c977e6283e9c6a145f509ca097f9a2746e7324b",
  "recorded_local_candidate_prefix": "a3d7fbc",
  "published_reconstruction": "3e67321ecde7921cd30fde93b97a7dca2405f0dd",
  "production_blob": "a0af62c143a2c924a73bdf54931439e198d1e8e0",
  "regression_blob": "b1ff13a8a6038b7a8ed183e5efca52cfb58cd4c6",
  "recorded_runtime": "Windows Server 2025 Datacenter 10.0.26100 amd64; Go 1.24.13; CGO_ENABLED=0",
  "tests_repeated_in_publication": false,
  "before": {
    "command": [
      "go",
      "test",
      "-json",
      "-count=1",
      "-short",
      "-p=2",
      "-timeout=4m",
      "-run",
      "^(TestTSBranchIdentityUniqueness|TestTSBranchHistoryIdentities)$",
      "."
    ],
    "exit_code": 1,
    "elapsed_seconds": 72.703,
    "events": [
      {
        "test": "TestTSBranchIdentityUniqueness/commit",
        "action": "fail"
      },
      {
        "test": "TestTSBranchIdentityUniqueness/branch",
        "action": "fail"
      },
      {
        "test": "TestTSBranchIdentityUniqueness",
        "action": "fail"
      },
      {
        "test": "TestTSBranchHistoryIdentities",
        "action": "fail"
      }
    ]
  },
  "after": {
    "command": [
      "go",
      "test",
      "-json",
      "-count=1",
      "-short",
      "-p=2",
      "-timeout=4m",
      "-run",
      "^(TestTSBranchIdentityUniqueness|TestTSBranchHistoryIdentities|TestBranchDiff|TestManualMergeConflict|TestTSDiffMerge_DiffBranches)$",
      "."
    ],
    "exit_code": 1,
    "elapsed_seconds": 17.0,
    "events": [
      {
        "test": "TestTSBranchIdentityUniqueness/branch",
        "action": "pass"
      },
      {
        "test": "TestTSBranchIdentityUniqueness/commit",
        "action": "pass"
      },
      {
        "test": "TestTSBranchIdentityUniqueness",
        "action": "pass"
      },
      {
        "test": "TestTSBranchHistoryIdentities",
        "action": "pass"
      },
      {
        "test": "TestBranchDiff",
        "action": "pass"
      },
      {
        "test": "TestManualMergeConflict",
        "action": "pass"
      },
      {
        "test": "TestTSDiffMerge_DiffBranches",
        "action": "fail"
      }
    ]
  }
}
```
