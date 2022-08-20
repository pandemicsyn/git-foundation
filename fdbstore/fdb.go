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
	objectOpKey  = "obj"
)

type FDBStore struct {
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

	s := &FDBStore{memStore.ModuleStorage, log, db, dir, make(map[string]subspace.Subspace)}

	// TODO: subspace this out further ?
	s.ss[refOpKey] = s.d.Sub(refOpKey)
	s.ss[objectOpKey] = s.d.Sub(objectOpKey)
	return s, nil
}

func (s *FDBStore) Remove() error {
	err := clear_subspace(s.db, s.d)
	return err
}

func (s *FDBStore) genStorageKey(op string) fdb.Key {
	return s.d.Pack(tuple.Tuple{op})
}
