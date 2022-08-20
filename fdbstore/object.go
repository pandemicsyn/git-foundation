package fdbstore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/utils/ioutil"
	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"
)

const ObjectChunkSize = 10000

var ErrUnsupportedObjectType = fmt.Errorf("unsupported object type")

// Temporarily uses a plumbing.MemoryObject, but should be replaced with a
// with a bespoke EncodedObject implementation later.
func (s *FDBStore) NewEncodedObject() plumbing.EncodedObject {
	return &plumbing.MemoryObject{}
}

type ObjectHeader struct {
	Type plumbing.ObjectType
	Size int64
}

// Store an EncodedObject in the FDBStore returning the Hash of the object and an error if any.
func (s *FDBStore) SetEncodedObject(o plumbing.EncodedObject) (plumbing.Hash, error) {
	h := o.Hash()
	if err := storeObject(s, o); err != nil {
		return plumbing.ZeroHash, err
	}
	return h, nil
}

// Split object into chunks of size ObjectChunkSize and store in the FDBStore.
func storeObject(s *FDBStore, o plumbing.EncodedObject) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		l := s.log.WithFields(logrus.Fields{"type": o.Type(), "hash": o.Hash(), "full_size": o.Size()})
		reader, err := o.Reader()
		if err != nil {
			return plumbing.ZeroHash, err
		}
		defer ioutil.CheckClose(reader, &err)
		r := bufio.NewReader(reader)
		buf := make([]byte, 0, ObjectChunkSize)

		part := 0
		for {
			pl := l.WithField("part", part)

			//read up to 10k bytes from the object into the read buffer
			n, err := io.ReadFull(r, buf[:cap(buf)])
			buf = buf[:n]
			if err != nil {
				if err == io.EOF {
					break
				}
				if err != io.ErrUnexpectedEOF {
					return plumbing.ZeroHash, err
				}
			}
			tr.Set(s.genObjectPartKey(o.Hash(), part), buf)
			pl.WithField("size", n).Debug("stored part")
			part++
		}

		header := ObjectHeader{
			Type: o.Type(),
			Size: o.Size(),
		}
		payload, err := json.Marshal(header)
		if err != nil {
			return nil, errors.Wrap(err, "failed to encode object header for storage")
		}
		tr.Set(s.genObjectKey(o.Hash(), "header"), payload)
		l.Debug("stored header object")
		return nil, nil
	})
	return err
}

func (s *FDBStore) HasEncodedObject(h plumbing.Hash) error {
	ret, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		ret = tr.Get(s.genObjectKey(h, "header")).MustGet()
		return
	})
	if err != nil {
		return err
	}
	if isNilKey(ret) {
		s.log.WithField("hash", h).Warn("object not found")
		return plumbing.ErrObjectNotFound
	}
	s.log.WithField("hash", h).Debug("object found")
	return nil
}

func (s *FDBStore) EncodedObjectSize(h plumbing.Hash) (size int64, err error) {
	ret, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		ret = tr.Get(s.genObjectKey(h, "header")).MustGet()
		return
	})
	if err != nil {
		return 0, err
	}
	if isNilKey(ret) {
		return 0, plumbing.ErrObjectNotFound
	}
	header := new(ObjectHeader)
	if err := json.Unmarshal(ret.([]byte), header); err != nil {
		return 0, errors.Wrap(err, "failed to unmarshal object header")
	}
	s.log.WithField("hash", h).WithField("size", header.Size).Debug("fetched object size")
	return header.Size, nil
}

func (s *FDBStore) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	if err := s.HasEncodedObject(h); err != nil {
		return nil, err
	}
	return s.getEncodedObject(t, h)
}

// TODO: redo using prefix query using FDBRangeKeys
func (s *FDBStore) getEncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	o := s.NewEncodedObject()
	o.SetType(t)
	_, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		header := new(ObjectHeader)
		ret = tr.Get(s.genObjectKey(h, "header")).MustGet()
		if isNilKey(ret) {
			s.log.WithField("hash", h).Warn("object not found on attempted get")
			return nil, plumbing.ErrObjectNotFound
		}
		if err := json.Unmarshal(ret.([]byte), header); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal object header")
		}
		o.SetSize(header.Size)
		for i := 0; i < int(header.Size/10000)+1; i++ {
			ret := tr.Get(s.genObjectPartKey(h, i)).MustGet()
			if isNilKey(ret) {
				s.log.WithField("part", i).WithField("hash", h).Warn("part not found")
				return nil, plumbing.ErrObjectNotFound
			}
			w, err := o.Writer()
			if err != nil {
				return nil, err
			}
			w.Write(ret)
		}
		return nil, nil
	})
	return o, err
}

func (s *FDBStore) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	s.log.Warn("IterEncodedObjects not implemented")
	var series []plumbing.EncodedObject
	switch t {
	case plumbing.AnyObject:
		series = []plumbing.EncodedObject{}
	case plumbing.CommitObject:
		series = []plumbing.EncodedObject{}
	case plumbing.TreeObject:
		series = []plumbing.EncodedObject{}
	case plumbing.BlobObject:
		series = []plumbing.EncodedObject{}
	case plumbing.TagObject:
		series = []plumbing.EncodedObject{}
	}
	return storer.NewEncodedObjectSliceIter(series), nil
}

func (s *FDBStore) ObjectPacks() ([]plumbing.Hash, error) {
	return nil, nil
}
func (s *FDBStore) DeleteOldObjectPackAndIndex(plumbing.Hash, time.Time) error {
	return nil
}

var errNotSupported = fmt.Errorf("not supported")

func (s *FDBStore) LooseObjectTime(hash plumbing.Hash) (time.Time, error) {
	return time.Time{}, errNotSupported
}
func (s *FDBStore) DeleteLooseObject(plumbing.Hash) error {
	return errNotSupported
}

// key = dir[url]/sub[obj]/tuple[hash, meta]
func (s *FDBStore) genObjectKey(h plumbing.Hash, meta string) fdb.Key {
	return s.ss[objectOpKey].Pack(tuple.Tuple{h.String(), meta})
}

// key = dir[url]/sub[obj]/tuple[hash, part]
func (s *FDBStore) genObjectPartKey(h plumbing.Hash, part int) fdb.Key {
	return s.ss[objectOpKey].Pack(tuple.Tuple{h.String(), part})
}
