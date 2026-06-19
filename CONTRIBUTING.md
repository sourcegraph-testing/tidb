# Contribution Guide

See the [Contribution Guide](https://github.com/pingcap/community/blob/master/CONTRIBUTING.md) in the
[community](https://github.com/pingcap/community) repo.

## Getting started

This is a [Go](https://golang.org/) project that uses Go modules and a
top-level `Makefile` for its build and test tooling.

Prerequisites:

- A working Go toolchain with `GOPATH` set (required by the `Makefile`).
- `make` and `git`.

Common tasks (run from the repository root):

- Build the TiDB server binary:

  ```sh
  make
  ```

  This is equivalent to `make server` and produces the `tidb-server` binary.
  To build the default package without the server, use `make build`.

- Run the unit tests:

  ```sh
  make test
  ```

  To run the Go tests directly without the surrounding tooling, use
  `make gotest`.

- Run the linters and static checks:

  ```sh
  make check
  ```

- Run the full developer workflow (checks plus tests), as CI does:

  ```sh
  make dev
  ```

See the `Makefile` for the complete list of available targets.
