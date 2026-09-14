# Experimental checkpoint lifecycle contracts

This directory contains the alternate checkpoint architecture's contracts:

- `crds/` mirrors the corrected central RunnerLease/RunnerExecution designs.
- `lifecycle/` tests conservative suspend/fence/workspace predicates.

There is no RunnerLease or RunnerExecution reconciler registered. These CRDs are
not required for the implemented coordinator-owned runtime, and applying them
alone creates no workers. Test with:

```sh
go test -race ./experimental/async-lease-broker/lifecycle
```

The connected runtime integration is in
`controllers/actions.github.com/asyncbroker.go`. With
`template.metadata.annotations.async.gheroco.dev/broker: enabled`, the ordinary
EphemeralRunner reconciler sends its JIT configuration to the administrator's HTTPS
broker, maps session readiness/status, and requests cancellation on deletion. It
bypasses ordinary runner pod creation for that template. Unannotated resources
retain the upstream path. Controller URL/token/CA come from process configuration,
never a workflow or URL annotation.

```sh
go test -race ./controllers/actions.github.com -run TestAsyncBroker -count=1
```

Use the central `gheroco/github-async-runners` deployment runbook for the matching
runner, broker, provider profile and dedicated-controller values. A live GHES canary
is still required. The coordinator runtime supports a restricted pinned action and
fails closed on broker loss; it does not restore a checkpointed .NET worker.
