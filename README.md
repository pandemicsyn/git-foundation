# git-foundation

just messing around with the go-git api - implementing a storage backend backed by fdb

Storer interface's implemented (not implemented pieces use git go's memory store implementations):

- [x] storer.ReferenceStorer
- [x] storer.ShallowStorer
- [x] storer.IndexStorer
- [x] config.ConfigStorer
- [ ] ModuleStore
- [ ] EncodedObjectStorer

See https://github.com/go-git/go-git/tree/master/plumbing/storer to figure out what this means.

# handy fdb cli commands while testing

```
# list first 25 keys
make ls-keys
--
# delete all the things!
fdb> writemode on
fdb> clearrange "" \xFF
fdb> getrange "" \xFF
```

# installing

foundationdb install is a bit...rough but this is working with the following

```
foundationdb 7.1.17 pre-release installed via debs
go get -u github.com/apple/foundationdb/bindings/go/src/fdb@release-7.1
fdb.MustAPIVersion(710)
```