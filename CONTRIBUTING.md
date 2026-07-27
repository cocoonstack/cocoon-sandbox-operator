# Contributing

Thanks for your interest in improving sandbox-operator!

## Before you start

- Read the [Code of Conduct](CODE_OF_CONDUCT.md).
- For anything beyond a small fix, open an issue first so we can agree on the
  direction before you invest in an implementation.

## Developer setup

Go 1.26+ is required.

```bash
make all         # fmt-check vet test build
make test-race   # unit tests with the race detector
make generate    # CRDs, RBAC, deepcopy (idempotent — commit its output)
make lint        # golangci-lint v2 when installed
```

The scaling benchmarks under `test/` (`scalebench`, `poolbench`, `l2bench`,
`l3bench`, `e2ebench`) back the performance claims in the README; if your
change touches a claimed code path, re-run the relevant benchmark and update
[PERFORMANCE.md](PERFORMANCE.md) rather than editing numbers by hand.

## Pull requests

- Keep changes focused; unrelated refactors belong in their own PR.
- Commit messages: a one-line summary, optionally followed by a body that
  explains *why* the change is needed.
- Tests should encode the intent of the change — if the business rule changes
  and the test still passes, the test is wrong. The provider/operator
  contracts documented in the README are pinned by tests on purpose; do not
  weaken them to make a change pass.
- CI (`.github/workflows/ci.yml`) must be green.

## Developer Certificate of Origin

Contributions are accepted under the
[Developer Certificate of Origin](https://developercertificate.org/). Sign off
your commits (`git commit -s`) to certify that you have the right to submit
the work under this repository's license.

## License

By contributing you agree that your contributions are licensed under
[AGPL-3.0](LICENSE). Provenance for the imported `agents.x-k8s.io` APIs
(Apache-2.0, headers retained) is tracked in [UPSTREAM.md](UPSTREAM.md).
