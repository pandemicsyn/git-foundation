package main

import (
	"bytes"
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func incrKey(tor fdb.Transactor, k fdb.Key) error {
	_, e := tor.Transact(func(tr fdb.Transaction) (interface{}, error) {
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, int64(1))
		if err != nil {
			return nil, err
		}
		one := buf.Bytes()
		tr.Add(k, one)
		return nil, nil
	})
	return e
}

func decrKey(tor fdb.Transactor, k fdb.Key) error {
	_, e := tor.Transact(func(tr fdb.Transaction) (interface{}, error) {
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, int64(-1))
		if err != nil {
			return nil, err
		}
		negativeOne := buf.Bytes()
		tr.Add(k, negativeOne)
		return nil, nil
	})
	return e
}

func getKey(tor fdb.Transactor, k fdb.Key) (int64, error) {
	val, e := tor.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return tr.Get(k).Get()
	})
	if e != nil {
		return 0, e
	}
	if val == nil {
		return 0, nil
	}
	byteVal := val.([]byte)
	var numVal int64
	readE := binary.Read(bytes.NewReader(byteVal), binary.LittleEndian, &numVal)
	if readE != nil {
		return 0, readE
	} else {
		return numVal, nil
	}
}
