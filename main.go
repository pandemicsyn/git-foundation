package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/memory"
)

const (
	refOpKey     = "ref"
	configOpKey  = "config"
	indexOpKey   = "index"
	shallowOpKey = "shallow"
)

func setupFDB() fdb.Database {
	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(620)
	// Open the default database from the system cluster
	return fdb.MustOpenDefault()
}

type Storage struct {
	memory.ObjectStorage
	memory.ModuleStorage
	log logrus.FieldLogger
	db  fdb.Database
	ns  string
	url string
}

func NewStorage(log logrus.FieldLogger, db fdb.Database, ns, url string) *Storage {
	memStore := memory.NewStorage()
	return &Storage{memStore.ObjectStorage, memStore.ModuleStorage, log, db, ns, url}
}

type SlowRef struct {
	Name   string
	Target string
}

func (s *Storage) Reference(n plumbing.ReferenceName) (*plumbing.Reference, error) {
	ret, err := s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		ret = tr.Get(fdb.Key(s.genRefKey(n))).MustGet()
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
		s.log.Debug("ref is 0")
		return nil, plumbing.ErrReferenceNotFound
	}
	s.log.Debug("ref: ", ref)

	r := new(SlowRef)
	if err = json.Unmarshal(ref, r); err != nil {
		s.log.WithError(err).Error("failed to unmarshal ref")
	}
	return plumbing.NewReferenceFromStrings(r.Name, r.Target), err
}

func (s *Storage) SetReference(r *plumbing.Reference) error {
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
		ierr := incrKey(tr, fdb.Key("global-ref-counter")) //TODO: fix hackity hack
		if ierr != nil {
			s.log.WithError(ierr).Error("failed to incr global refs counter")
		}
		return
	})
	return err
}

func (s *Storage) CheckAndSetReference(r, old *plumbing.Reference) error {
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

func (s *Storage) CountLooseRefs() (int, error) {
	//TODO: don't use just a single global ref counter (just a quick hack to satisfy the storer iface)
	c, err := getKey(s.db, fdb.Key("global-ref-counter"))
	return int(c), err
}

func (s *Storage) PackRefs() error {
	s.log.Info("pack refs called")
	return nil
}

func (s *Storage) RemoveReference(n plumbing.ReferenceName) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Clear(fdb.Key(s.genRefKey(n)))
		derr := decrKey(tr, fdb.Key("global-ref-counter")) //TODO: fix hackity hack
		s.log.WithError(derr).Warning("decr failed")
		return
	})
	return err
}

func (s *Storage) IterReferences() (storer.ReferenceIter, error) {
	//TODO: make this an actual iter, don't just read all refs into a slice

	refs := make([]*plumbing.Reference, 0)

	prefixKey := fmt.Sprintf("%s.%s.%s", s.ns, refOpKey, s.url)

	rkey, err := fdb.PrefixRange([]byte(prefixKey))
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure prefix key for refs iter")
	}

	_, err = s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
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

func (s *Storage) genRefKey(n plumbing.ReferenceName) string {
	return fmt.Sprintf("%s.%s.%s.%s", s.ns, refOpKey, s.url, n)
}

func (s *Storage) Config() (*config.Config, error) {
	ret, err := s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		ret = tr.Get(fdb.Key(s.genConfigKey())).MustGet()
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

func (s *Storage) SetConfig(r *config.Config) error {
	payload, err := json.Marshal(r)
	if err != nil {
		return errors.Wrap(err, "failed to encode config")
	}
	_, err = s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(fdb.Key(s.genConfigKey()), payload)
		return
	})
	return err
}

func (s *Storage) genConfigKey() string {
	return s.genStorageKey(configOpKey)
}

func (s *Storage) Index() (*index.Index, error) {
	ret, err := s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
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

func (s *Storage) SetIndex(i *index.Index) error {
	payload, err := json.Marshal(i)
	if err != nil {
		return errors.Wrap(err, "failed to encode index")
	}
	_, err = s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(fdb.Key(s.genIndexKey()), payload)
		return
	})
	return err
}

func (s *Storage) genIndexKey() string {
	return s.genStorageKey(indexOpKey)
}

func (s *Storage) Shallow() ([]plumbing.Hash, error) {
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

func (s *Storage) SetShallow(hash []plumbing.Hash) error {
	payload, err := json.Marshal(hash)
	if err != nil {
		return errors.Wrap(err, "failed to encode hash")
	}
	_, err = s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(fdb.Key(s.genShallowKey()), payload)
		return
	})
	return err
}

func (s *Storage) genShallowKey() string {
	return s.genStorageKey(shallowOpKey)
}

func (s *Storage) genStorageKey(op string) string {
	return fmt.Sprintf("%s.%s.%s.%s", s.ns, configOpKey, s.url, op)
}

func main() {
	action := os.Args[1]
	url := "https://github.com/pandemicsyn/git-foundation.git"
	db := setupFDB()

	l := logrus.New()
	l.Level = logrus.DebugLevel

	s := NewStorage(l, db, "testspace", url)

	switch action {
	case "clone":
		clone(l, s, url)
		log(l, s)
	case "log":
		log(l, s)
	default:
		l.Error("invalid opt")
		return
	}
}

func clone(l logrus.FieldLogger, s storage.Storer, url string) {
	// Clone the given repository, all the objects, references and
	// configuration sush as remotes, are save into the Aerospike database
	// using the custom storer
	l.Info("git clone ", url)

	_, err := git.Clone(s, nil, &git.CloneOptions{
		URL:      url,
		Progress: os.Stdout,
	})
	if err != nil {
		l.WithError(err).Fatal("clone failed")
	}
}

func log(l logrus.FieldLogger, s storage.Storer) {
	// We open the repository using as storer the custom implementation
	r, err := git.Open(s, nil)
	if err != nil {
		l.Fatal(err)
	}

	// Prints the history of the repository starting in the current HEAD
	l.Info("git log --oneline")

	ref, err := r.Head()
	if err != nil {
		l.Fatal(err)
	}
	l.Info("head hash", ref.Hash())

	cIter, err := r.Log(&git.LogOptions{From: ref.Hash()})
	if err != nil {
		l.Fatal(err)
	}

	// ... just iterates over the commits, printing it
	err = cIter.ForEach(func(c *object.Commit) error {
		fmt.Println(c)
		return nil
	})
	if err != nil {
		l.Fatal(err)
	}
}
