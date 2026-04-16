## Overview

# Contributing to Platform Mesh

We want to make contributing to this project as easy and transparent as possible.

## Our development process

We use GitHub to track issues and feature requests, as well as accept pull requests.

## Pull requests

You are welcome to contribute with your pull requests. These steps explain the contribution process:

1. Fork the repository and create your branch from `main`.
1. Add or update tests for your change.
1. If you've changed APIs or generated outputs, regenerate them and review the diffs.
1. Make sure the local verification steps pass.
1. Sign the Developer Certificate of Origin (DCO).

## Development setup

`search-operator` uses [Task](https://taskfile.dev) for build automation:

```bash
task fmt
task lint
task test
task cover
task validate
task generate
task run
```

## Code style

- Follow the existing split between controller wiring, lifecycle subroutines, config, and OpenSearch integration.
- Prefer small, focused changes over broad refactors.
- Add tests next to the code you change.
- Regenerate API-derived files instead of editing generated output manually.

## Testing

> **NOTE:** You should always add tests if you are adding code to our repository.

Run the standard local test flow with:

```bash
task test
```

If you change API types or CRD-related resources, also run:

```bash
task generate
```

## DCO

All contributions to this repository must be signed off in compliance with the [Developer Certificate of Origin (DCO)](https://developercertificate.org/).

## Code of Conduct

Please refer to our [Code of Conduct](https://github.com/platform-mesh/.github/blob/main/CODE_OF_CONDUCT.md) for information on the expected conduct for contributing to Platform Mesh.

## Generative AI

If you use generative AI tools while preparing a contribution, you remain responsible for the correctness, safety, licensing, and maintainability of the submitted change.

If you use Claude while contributing, do not assume it will automatically pick up this repository's `AGENTS.md`. Explicitly provide or reference `AGENTS.md` at the start of the session so Claude has the repository-specific instructions before it suggests or applies changes.

## License

By contributing to Platform Mesh, you agree that your contributions will be licensed under its [Apache-2.0 license](LICENSE).
