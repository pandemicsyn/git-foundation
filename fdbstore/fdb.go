package fdbstore

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/sirupsen/logrus"
)

const (
	refOpKey     = "ref"
	configOpKey  = "config"
	indexOpKey   = "index"
	shallowOpKey = "shallow"
)

type FDBStore struct {
	memory.ObjectStorage
	memory.ModuleStorage
	log logrus.FieldLogger
	db  fdb.Database
	d   directory.DirectorySubspace
	ss  map[string]subspace.Subspace
}

func NewStorage(log logrus.FieldLogger, db fdb.Database, ns, url string) (*FDBStore, error) {
	memStore := memory.NewStorage()
	var err error
	dir, err := directory.CreateOrOpen(db, []string{url}, nil)
	if err != nil {
		return nil, err
	}
	s := &FDBStore{memStore.ObjectStorage, memStore.ModuleStorage, log, db, dir, make(map[string]subspace.Subspace)}

	// TODO: subspace this out further ?
	s.ss[refOpKey] = s.d.Sub(refOpKey)
	return s, nil
}

func (s *FDBStore) genStorageKey(op string) fdb.Key {
	return s.d.Pack(tuple.Tuple{op})
}
