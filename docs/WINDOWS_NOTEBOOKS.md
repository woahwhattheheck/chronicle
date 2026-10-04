# Native Windows notebook follow-through

This supplements [the Windows compatibility report](WINDOWS.md) for PR #104.
It is a focused notebook result, not a passing repository-wide suite.

## Execution

[Run 37192665685](https://github.com/woahwhattheheck/chronicle/actions/runs/37192665685)
completed successfully on Windows Server 2025 (`10.0.26100`, amd64), using
Go 1.24.13. Its only test command was:

```powershell
go test -count=1 -timeout=120s -json -run '^TestNotebook' .
```

All **15 selected top-level tests passed**, with no selected failures. The
selection included notebook storage, dirty tracking, sharing, create/list/delete,
cell execution, HTML export, markdown parsing, capacity limits and a generated-ID
regression. The regression allocated 128 notebooks and 256 cells across eight
concurrent engines without duplicate IDs on that candidate.

The execution started from product source `f929136a0ee36c3c9693496c48582ffdd5d6e288`
and applied the recorded two-file patch. The exact tested candidate is retained
at `91281b725d6ec1b749c1d5112a9d73a1434b5388`; its workflow head was
`4fe2c3c3a6dccfef699e60b09c87916cb9a60f84`.

## What was composed into the original submission

`ParseMarkdown` retained a stale pointer to the earlier markdown cell after a
fenced query, silently discarding the following prose. Product commit
`ba885f2a6f2717b265dd4e1750d8ee760962a03e` resets that state when the fence closes.
Commit `a5fa154af8f883d351e6a312a6bd9479f8bc7fc9` strengthens the existing fixture
to require all four cells and its previously lost `More text here` paragraph.
The published parser function and that assertion are identical to those exercised
by the native run.

An independent notebook/cell ID collision-probe repair reached the original
branch while this run was in progress. That implementation was preserved. The
candidate's alternative atomic-ID implementation and its cross-engine regression
were **not** copied over it. Accordingly, the 15-pass result belongs to the
isolated candidate, not to the final combined branch. The combined branch and
repository-wide suite were not rerun for this documentation or parser composition.

## Retained evidence

[Artifact 11299815995](https://github.com/woahwhattheheck/chronicle/actions/runs/37192665685/artifacts/11299815995)
contains the source patch, source files, raw JSON test events, stderr, source and
platform provenance, and candidate commit. ZIP SHA-256:
`b1940be8286dc43fe3f00c43f4efeb885464c3c846d89a78a0d79b495f771f86`.
The artifact has a seven-day retention period; the committed source, run pins,
result summary and scope distinction above remain in this report.

No race-detector, C-FFI, full-suite, performance benchmark, upstream CI approval,
maintainer acceptance or bounty-payment result is asserted by this run.
