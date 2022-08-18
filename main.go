package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pandemicsyn/git-foundation/fdbstore"
	"github.com/sirupsen/logrus"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
)

func setupFDB() fdb.Database {
	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(710)
	// Open the default database from the system cluster
	return fdb.MustOpenDefault()
}

func canaryWriteRead(l logrus.FieldLogger, db fdb.Database, s *fdbstore.FDBStore) {
	_, err := db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(fdb.Key("hello"), []byte("world"))
		return
	})
	if err != nil {
		l.Fatalf("Unable to set FDB database value (%v)", err)
	}

	ret, err := db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		ret = tr.Get(fdb.Key("hello")).MustGet()
		return
	})
	if err != nil {
		l.Fatalf("Unable to read FDB database value (%v)", err)
	}

	v := ret.([]byte)
	if string(v) != "world" {
		l.Fatalf("FDB database value is not correct (%v)", v)
	}
}

func main() {

	var url string
	var runClone bool
	flag.StringVar(&url, "url", "https://github.com/pandemicsyn/git-foundation.git", "url to clone")
	flag.BoolVar(&runClone, "run-clone", true, "whether to run the clone")
	flag.Parse()

	db := setupFDB()

	l := logrus.New()
	l.Level = logrus.DebugLevel

	s, err := fdbstore.NewStorage(l, db, "testspace", url)
	if err != nil {
		l.WithError(err).Fatal("unable to initalize fdb based store")
	}

	canaryWriteRead(l, db, s)

	if runClone {
		clone(l, s, url)
	}

	l.Info("clone complete")

	log(l, s)

	// test fdb write

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
