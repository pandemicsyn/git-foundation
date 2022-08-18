package fdbstore

import (
	"encoding/json"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/go-git/go-git/v5/config"
	"github.com/pkg/errors"
)

func (s *FDBStore) Config() (*config.Config, error) {
	ret, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		ret = tr.Get(s.genConfigKey()).MustGet()
		return
	})
	if err != nil {
		return nil, err
	}
	i := new(config.Config)

	if ret == nil {
		return config.NewConfig(), nil
	}
	c := ret.([]byte)
	if len(c) == 0 {
		return config.NewConfig(), nil
	}
	if err = json.Unmarshal(c, i); err != nil {
		s.log.WithError(err).Error("config unmarshal failed")
	}
	return i, err
}

func (s *FDBStore) SetConfig(r *config.Config) error {
	payload, err := json.Marshal(r)
	if err != nil {
		return errors.Wrap(err, "failed to encode config")
	}
	_, err = s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(s.genConfigKey(), payload)
		return
	})
	return err
}

func (s *FDBStore) genConfigKey() fdb.Key {
	return s.genStorageKey(configOpKey)
}
