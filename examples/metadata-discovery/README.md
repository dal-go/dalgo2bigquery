# Initial metadata discovery harness

Run the offline fixture through the production guarded Go discovery client:

```sh
go run ./examples/metadata-discovery --fixture --source fixture-source
```

The fixture transport has no network implementation or credentials. It returns
synthetic dataset/table metadata with deliberately hostile nested descriptions,
policy resources and expressions; stdout contains only the bounded partial public
envelope with `provenance.kind: synthetic-fixture`. Fixture output is test evidence,
never observed live registry evidence. No source admission changes here.

Without `--fixture`, inject `providerFactory` from an operator-owned build-tagged
file. The command refuses if the factory is absent; it does not discover ADC,
load credential files, start a listener or create a ledger. The factory receives
a 15-second setup context and must honor cancellation. It returns:

- A reviewed exact `DiscoveryConfig.Allowlist` (no wildcard, redirect or URL input).
- A `DiscoveryAuthorize` callback reading protected current owner/consent/source/
  session state and returning its private principal plus policy revision.
- An identity-attesting `Provider`, a trusted transport and `EvidenceKind`.

The callback must reject sign-out, consent withdrawal and changed source/session
binding. Its policy revision changes whenever that authorization binding changes.
Identity/token generations must be attested independently of mere token presence.
The client compares the original binding before each physical metadata dispatch,
between dataset/table calls and immediately before delivery. Exceptions and API
failures are sanitized; policy is rechecked after identity callbacks return so
withdrawal during provider attestation refuses dispatch or delivery. Requests
cannot dispatch based on an upper check while queued: a discovery-only physical
transport wrapper reattests identity and policy inside the admitted worker just
before invoking the trusted transport. Responses
have decoded-byte bounds before JSON parsing. Encoded gzip wire bytes do not
have a separate byte cap; transport/body lifetime is bounded by the HTTP and
total deadlines. The CLI uses a
30-second total discovery limit, 15-second per-HTTP limit, 1 MiB per response and
2 MiB cumulative response limit; public output has its own 64 KiB ceiling.

Only `datasets.get` with METADATA and `tables.get` with STORAGE_STATS are composed.
No job, query/dry-run, rows, list, result snapshot, materialization or query ledger
path exists in this harness. Public projection publication still needs source
rights/provenance review. All query/cost/rights/retention gates remain blocked.

Metadata schema projection is names/native types/normalized modes/nested fields
only. Precision, scale, range descriptors and every other optional property are
omitted in draft-1, even when present. This makes RANGE and parameterized decimal
observations partial; consumers cannot compile execution profiles from them.

The two API metadata reads are not a transactional source snapshot. The receipt
records a bounded observation; later source admission requires independent native
configuration validation and current identity, source, rights and cost evidence.
