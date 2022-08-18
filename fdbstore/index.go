package fdbstore

import (
	"encoding/json"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/pkg/errors"
)

func (s *FDBStore) Index() (*index.Index, error) {
	ret, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		ret = tr.Get(fdb.Key(s.genIndexKey())).MustGet()
		return
	})
	if err != nil {
		return nil, err
	}
	i := new(index.Index)
	if err = json.Unmarshal(ret.([]byte), i); err != nil {
		s.log.WithError(err).Error("failed to unmarshal index")
	}
	return i, err
}

func (s *FDBStore) SetIndex(i *index.Index) error {
	payload, err := json.Marshal(i)
	if err != nil {
		return errors.Wrap(err, "failed to encode index")
	}
	_, err = s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(s.genIndexKey(), payload)
		return
	})
	return err
}

func (s *FDBStore) genIndexKey() fdb.Key {
	return s.genStorageKey(indexOpKey)
}
