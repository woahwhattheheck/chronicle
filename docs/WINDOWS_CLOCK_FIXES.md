# Windows clock-collision and delta-sync repairs

Follow-through for the existing Windows compatibility submission, PR #104 / testing bounty #9. These changes are published on `bounty-9-windows-testing`; this document does not assert native Windows acceptance or a bounty award.

## Published repairs

| Component | Commit | Change |
| --- | --- | --- |
| Notebooks | `6be8f377886cfb8a7586c270ab466a52279244c1` | Allocate notebook and cell IDs under their existing lifecycle lock, skipping IDs already present. Preserve explicitly supplied IDs, imported records, numeric ID formats and capacity checks. |
| Alert builder | `754325e1bfd014321e56f14cca10cd6199cb7848` | Use a process-monotonic atomic allocator shared by generated rule and trigger IDs. Repeated or backward clock readings no longer reuse a generated ID. Preserve the `alert-<number>` format and existing import/validation behavior. |
| Delta sync | `bfd2f4e5959a089cb19a2a92c1277979c0c02a97` | Allocate unique process-local numeric batch IDs; keep each request context alive until the HTTP attempt completes; requeue both the failed offline batch and its unattempted suffix through the existing queue-capacity policy. |
| Anomaly explanations | `2b9080f674a5d7c837291b750e825951ee8b8478` | Choose an unused `expl-<number>` ID and insert the explanation under the same existing registry lock, preserving other explanations, cache policy and generated content. |

The atomic allocators guarantee distinct generated IDs within a process, not coordination between independent processes or persistent ID reservation across restarts. The notebook and explanation allocators avoid collisions with records currently in their registries.

## Retained native evidence

The historical failure targets below come from fork Actions artifact `11297698322`. The downloaded archive has SHA-256:

```
8f36853d49bbdf505e93fa75afe917b4de0edda6b0170947d926409cd35e1221
```

Its embedded `provenance.json` identifies source `006bc101c1f2079b04696dfc497f5587328fcc46`, Windows Server 2025 / amd64, Go 1.24.13 and `CGO_ENABLED=0`. Its recorded command was:

```
go test -short -count=1 -p=2 -timeout=120s -json ./...
```

This is an earlier execution, not a run of the commits above.

| Historical failure | Observed result | Repair area |
| --- | --- | --- |
| `TestNotebookEngineGetAndList` | NB1 replaced by NB2; expected two notebooks, received one. | Notebook registry IDs. |
| `TestAlertBuilder_ListRules` | Expected at least two rules, received one. | Shared alert IDs. |
| `TestAlertBuilder_ExportImport` | Expected two imported rules, received one. | Shared alert IDs before export. |
| `TestAlertBuilder_MaxRulesLimit` | Expected a per-user limit error, but overwritten records kept the count low. | Shared alert IDs. |
| `TestDeltaSyncManager_Batching` | Duplicate batch IDs in a three-batch split. | Delta batch IDs. |
| `TestAnomalyExplainabilityStats` | Expected three explanations, received one; metric-filtered and limited histories also failed. | Explanation registry IDs. |

The premature request cancellation and dropped offline-queue suffix were separate source-inspection findings. They are not claimed as failures demonstrated by that historical execution.

## Validation boundary

One focused local notebook lifecycle replay used Go 1.23.2, the complete before/after `notebooks.go` source, a fixture-only frozen clock, and compile-only database collaborators. Three of ten checks passed before the repair; all ten passed afterward. The checks covered generated notebook/cell IDs, preservation of existing/imported records, explicit IDs, limits, correct cell selection and concurrent notebook creation. Database execution was excluded.

No additional test or build run was performed for the alert, delta-sync or explanation repairs. There is no new native Windows or whole-repository result attached to these commits, and the focused notebook result must not be represented as one.

The archive also contains failures unrelated to identifier collisions. For example, `TestStreamDSLV2Stats` observed zero events/second before a measurable elapsed interval, while `TestModelRegistry` reported that TinyML requires the `experimental` build tag. These changes neither alter those contracts nor establish the current status of other independently repaired files. Acceptance requires the existing integration owner to evaluate the composed submission at its actual head; the historical archive alone cannot establish that it is green.
