package fdbstore

import (
	"encoding/json"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/pkg/errors"
)

func (s *FDBStore) Shallow() ([]plumbing.Hash, error) {
	ret, err := s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		ret = tr.Get(fdb.Key(s.genShallowKey())).MustGet()
		return
	})
	if err != nil {
		return nil, err
	}
	var h []plumbing.Hash
	if err = json.Unmarshal(ret.([]byte), &h); err != nil {
		s.log.WithError(err).Error("failed to unmarshal shallow")
	}
	return h, err
}

func (s *FDBStore) SetShallow(hash []plumbing.Hash) error {
	payload, err := json.Marshal(hash)
	if err != nil {
		return errors.Wrap(err, "failed to encode hash")
	}
	_, err = s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(s.genShallowKey(), payload)
		return
	})
	return err
}

func (s *FDBStore) genShallowKey() fdb.Key {
	return s.genStorageKey(shallowOpKey)
}
