# Operator-owned composition scaffold

`go run ./examples/operator-server --help` compiles the actual composition. An
ordinary invocation refuses before creating a ledger or listener because no
trusted provider is configured. The repository never reads credentials.

An operator adds a local build-tagged `operator.go` in this directory and sets
`providerFactory` in `init()`. That factory returns `operatorConfig` with a reviewed
profile, protected compiled plan, current policy `Prepare`, trusted verified
`Provider`, authenticated transport and explicit approved cost/session principal.
It must not manufacture an attestation from a token or configuration label. Setup
must honor its 15-second context. The operator owns this integration and its
credential lifecycle; it is not a general ADC or OAuth implementation.

After review of that operator integration, its invocation is:

```sh
go run -tags operator ./examples/operator-server \
  --ledger /absolute/private/session \
  --listen 127.0.0.1:8080 --origin http://localhost:4200
```

`main` owns signal cancellation, private persistent ledger creation, client and
handler construction, and the loopback server's shutdown. No listener starts on
import or in tests. Composition tests inject the trusted factory and a recording
serve function; handler tests exercise metadata, dry run, approval and same-job
pages through the production HTTP routes without cloud access or a live port.
The actual authorized local-server journey remains a later acceptance gate.
