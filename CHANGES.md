# Changes from upstream

This repository is a frozen fork of etcd used for Sourcegraph QA pipelines. The following directive was added to the `go.mod` file in the following commits. The rest of the repository content is the same as upstream (but frozen).

```
replace (
    go.uber.org/zap => github.com/sourcegraph-testing/zap v1.14.1
)
```

`2f9a487ebbd2f1a46b5f2c2262ae8f0ef4c4d42f` -> `8eaaa098b4e938b18485f7b1fa7d8e720b04c699`
`43764a59b7dcb846dc1e9754e8f125818c69a96f` -> `b5f100a179e20d5539e629bd0919d05774cb7c6a`
`b4f42abc36d893ec3f443af78fc62705a2e54236` -> `9aab49176993f9dc0ed2fcb9ef7e5125518e8b98`

