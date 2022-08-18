package fdbstore

import (
	"encoding/json"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage"
	"github.com/pkg/errors"
)

type SlowRef struct {
	Name   string
	Target string
}

func (s *FDBStore) Reference(n plumbing.ReferenceName) (*plumbing.Reference, error) {
	ret, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		ret = tr.Get(s.genRefKey(n)).MustGet()
		return
	})
	if err != nil {
		return nil, err
	}

	if ret == nil {
		s.log.Debug("ret is nil")
		return nil, plumbing.ErrReferenceNotFound
	}

	ref := ret.([]byte)
	if len(ref) == 0 {
		s.log.WithField("ref_name", n).Debug("ref is 0")
		return nil, plumbing.ErrReferenceNotFound
	}
	s.log.Debugf("ref: %s", ref)

	r := new(SlowRef)
	if err = json.Unmarshal(ref, r); err != nil {
		s.log.WithError(err).Error("failed to unmarshal ref")
	}
	return plumbing.NewReferenceFromStrings(r.Name, r.Target), err
}

func (s *FDBStore) SetReference(r *plumbing.Reference) error {
	raw := r.Strings()
	payload, err := json.Marshal(SlowRef{
		Name:   raw[0],
		Target: raw[1],
	})
	if err != nil {
		return errors.Wrap(err, "failed to encode ref")
	}
	_, err = s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(fdb.Key(s.genRefKey(r.Name())), payload)
		ierr := incrKey(tr, s.genRefCounterKey()) //TODO: fix hackity hack
		if ierr != nil {
			s.log.WithError(ierr).Error("failed to incr global refs counter")
		}
		return
	})
	return err
}

func (s *FDBStore) CheckAndSetReference(r, old *plumbing.Reference) error {
	//TODO: actually do a in a proper transact/cas op
	// just reusing the existing calls because im just fucking around
	if r == nil {
		return nil
	}

	if old != nil {
		tmp, err := s.Reference(r.Name())
		if err != nil {
			return errors.Wrap(err, "failed fetching ref for cas")
		}
		if tmp != nil && tmp.Hash() != old.Hash() {
			return storage.ErrReferenceHasChanged
		}
	}
	return s.SetReference(r)
}

func (s *FDBStore) CountLooseRefs() (int, error) {
	//TODO: fix this - just a quick hack to satisfy the storer iface
	c, err := getKey(s.db, s.genRefCounterKey())
	s.log.WithField("count", c).Debug("CoundLooseRefs called")
	return int(c), err
}

func (s *FDBStore) PackRefs() error {
	s.log.Info("pack refs called")
	return nil
}

func (s *FDBStore) RemoveReference(n plumbing.ReferenceName) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Clear(s.genRefKey(n))
		derr := decrKey(tr, s.genRefCounterKey())
		s.log.WithError(derr).Warning("decr failed")
		return
	})
	return err
}

func (s *FDBStore) IterReferences() (storer.ReferenceIter, error) {
	//TODO: make this an actual iter, don't just read all refs into a slice

	refs := make([]*plumbing.Reference, 0)

	//prefixKey := fmt.Sprintf("%s.%s.%s", s.ns, refOpKey, s.url)

	rkey, err := fdb.PrefixRange(s.ss[refOpKey].FDBKey())
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure prefix key for refs iter")
	}

	_, err = s.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		resp, err := tr.GetRange(rkey, fdb.RangeOptions{}).GetSliceWithError()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get slice on refs iter get rance call")
		}
		if len(resp) == 0 {
			s.log.Warning("refs iter call has empty resp")
			return
		}
		for i := range resp {
			//TODO: this is terrible
			r := new(SlowRef)
			if err = json.Unmarshal(resp[i].Value, r); err != nil {
				s.log.WithError(err).Error("unmarshal in iter ref failed")
				return nil, err
			}
			refs = append(refs, plumbing.NewReferenceFromStrings(r.Name, r.Target))
		}
		return nil, nil
	})
	return storer.NewReferenceSliceIter(refs), err
}

// key = dir[url]/sub[refs]/tuple[reference name]
func (s *FDBStore) genRefKey(n plumbing.ReferenceName) fdb.Key {
	return s.ss[refOpKey].Pack(tuple.Tuple{n.String()})
}

func (s *FDBStore) genRefCounterKey() fdb.Key {
	return s.d.Pack(tuple.Tuple{"refs-counter"})
}
