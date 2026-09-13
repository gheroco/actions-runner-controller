# Experimental async lifecycle contracts

This directory is intentionally outside the registered ARC controllers.

- `crds/` mirrors the corrected contracts in `gheroco/github-async-runners/spec`.
- `lifecycle/` provides tested conservative worker lifecycle decisions: no delete
  before durable suspend commit; no resume before callback, old-worker deletion,
  workspace release, matching epoch and matching generation; cancellation or
  timeout requires finalization instead of ordinary resume.

There is NO RunnerLease or RunnerExecution reconciler registered in this branch.
Applying these CRDs alone does not create workers. Standard ARC behavior is
unchanged. The predicates do not fence Kubernetes writes by themselves: production
wiring needs a durable execution claim and authenticated worker-side epoch checks.

Test: `go test -race ./experimental/async-lease-broker/lifecycle`.

See the central deployment runbook and implementation status before deploying.
