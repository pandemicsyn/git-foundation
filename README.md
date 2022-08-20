# git-foundation - a FoundationDB backend for go-git

A [FoundationDB](https://www.foundationdb.org/) storage backend for use with [go-git](https://github.com/go-git/go-git).

This package is a WIP and is a naive implementation of go-git's storer interface. Its roughly modeled on the go-git memory implementation but obviously using FoundationDB instead of in mem semantics.

Current implementation status:

- [x] storer.ReferenceStorer
- [x] storer.ShallowStorer
- [x] storer.IndexStorer
- [x] config.ConfigStorer
- [ ] ModuleStore
- [ ] EncodedObjectStorer (Mostly working including sharding objects within foundation, missing IterEncodedObjects implementation)

See https://github.com/go-git/go-git/tree/master/plumbing/storer to figure out what this means.

> **Note**
> I don't get to work with Go day to day anymore, this package mostly exists as an execuse for me to write some Go.

# Installation

foundationdb install is a bit...rough but this is working with the following

```
foundationdb 7.1.17 pre-release installed via debs
go get -u github.com/apple/foundationdb/bindings/go/src/fdb@release-7.1
go get .
```

## handy fdb cli commands while testing

```
# list first 25 keys
make ls-keys
--
# delete all the things!
fdb> writemode on
fdb> clearrange "" \xFF
fdb> getrange "" \xFF
```
