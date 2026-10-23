# OrangeOrder in Go

[中文](README.zh.md)

HTTP is the standard `net/http`. Each process is goroutines plus a lock. sqlite is the pure-Go module `modernc.org/sqlite`, so cgo is not required. Module checksums are in `go.sum`.

## Run

Go 1.22 is required.

```bash
go/run.sh --root .
```

```bash
go/run.sh test
```

`go/run.sh` builds `go/bin/orange`.

Shared rules for ports, acceptance, and the file map are on the [homepage](../README.md).
