# dalgo2bigquery

Go building blocks for the accepted DataTug BigQuery analytical contract. This first tranche implements bounded lossless JSON validation, exact scalar cell normalization, restricted RFC8785 JCS and explicit named hash projections, and original execution-deadline calculations. It is not an executable BigQuery adapter yet.

The governing [A0 contract](https://github.com/datatug/datatug/blob/a541f8da3d1b2db9ffa5c2e5396efa2afa768066/spec/research/public-data-fabric/evidence/bigquery-dual-runtime/a0/accepted-contract.md) and [approved plan](https://github.com/sneat-co/workbench/blob/9fd225742a6eeb6cf439ef935977a8c6c2dba550/spec/plans/github.com/datatug/datatug/public-data-fabric-bigquery/README.md) require both Go and JavaScript. Released DALgo v0.89.6, record v0.1.4 and Google API SDK v0.296.0 are pinned without replacements. SDK/policy tests use in-memory transport, never credentials or real jobs.

## Contract corpus

`testdata/contract/manifest.json` binds revision1 initial scenarios by SHA256. This corpus is owned here; JavaScript vendors exact committed bytes with immutable source commit/hash. Additions require a new revision and both runtime reports. These initial canonical/value/row cases are a first subset, not the final full request/response state-machine corpus. Fields named `input` contain literal raw JSON strings; they must not be parsed/reserialized before the production parser sees them. Every scenario has a stable ID, kind, expected canonical/digest/value or sanitized error. Both runtimes must preserve scalar exact strings and distinguish SQL NULL from JSON text null.

Named payload field spelling is frozen in `HashPayload`: ReadPlan and SourceProfile use A0 fields; Observation binding uses `table/location/type/config/schema`; Approval uses `effectivePlanDigest/observationDigest/policyDigest/principal/jobProject/location/maximumBytesBilled/sessionBudgetBytes/bounds/estimatedBytes`. Only explicit top-level volatile fields are omitted. This does not validate complete typed profile/plan eligibility; the subsequent compiler/metadata gates do that. All numeric JSON tokens in adapter-owned hashes must be exact integer lexical tokens within the JS safe range; warehouse numbers remain strings. Raw provider JSON preserves every numeric token without floating conversion. No Unicode normalization is performed.

`OperationDeadline` computes per-operation bounds from the original trusted-ledger absolute deadline. It does not persist a ledger or authorize control operations; production status/cancel orchestration must enforce its explicit capability, unchanged cumulative counters and billing reservation rules.

## Next tranches and acceptance

1. Complete schema/precision validation and query guards/compiler, then native TABLE metadata eligibility and bounded authenticated SDK transport, approval bindings, private atomic budget/nonce/deadline ledger.
2. Single-dispatch capped jobs, status/cancel, schema-bearing results, same-job and partial-page cursors, recordset bridge and injected local server harness. Expand the corpus with exact requests/status/headers/raw bodies and expected dispatch counts.
3. Tagged driver release before actual CLI integration; browser package parity at both core refs, source-access profile and rights/admission review.
4. Independent actual CLI/local-server and deployed-browser metadata → dry run → approved cap → two same-job pages, with operator-owned identity/project and concrete spending cap. Exact CI/release/deploy/landing/cleanup receipts remain required.

All six approved-plan tasks remain incomplete; this first tranche advances Task1 corpus preparation and Task2 value/deadline primitives only. Root owns landing. No source admission, live runtime success, budget persistence, approved paid execution or cancellation receipt is claimed.
