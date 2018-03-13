package main

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"./pgwal"
)

type RecoverOpts struct {
	Dir      string
	Target   string // "0/123456:5432"
	Timeline int
}

func (a Agent) Recover(opts *RecoverOpts) {

	// --timeline 1
	// --target 0/123456:4321 (lsn:txid)
	// --dir my-recovered-db/

	lsn := uint64(0xffffffffffff) // XXX: parse opts.Target
	//txID := uint64(999999)

	files, err := a.store.List()
	if err != nil {
		log.Fatal(err)
	}

	var baseLSN uint64
	var baseTs int64
	var baseTimeline int
	for _, f := range files {
		var lsn0 uint64
		var timeline0 int
		var ts0 int64
		fmt.Sscanf(f.Name, "%012x.%x.%x.", &lsn0, &timeline0, &ts0)
		if lsn0 > 0 && lsn0 <= lsn && timeline0 <= opts.Timeline && strings.HasSuffix(f.Name, ".base") {
			baseLSN = lsn0
			baseTimeline = timeline0
			baseTs = ts0
		}
	}

	if baseLSN == 0 {
		log.Fatal("no base available to recover @", pgwal.LSN(lsn))
	}

	log.Print("pgbackup: using base @", pgwal.LSN(baseLSN), ", ", time.Since(time.Unix(baseTs, 0)).Truncate(time.Second), " ago")

	err = os.MkdirAll(opts.Dir, 0700)
	if err != nil {
		log.Fatal(err)
	}

	if !strings.HasSuffix(opts.Dir, "/") {
		opts.Dir = opts.Dir + "/"
	}

	r := &multiPartReader{
		Store: a.store,
		Name:  fmt.Sprintf("%012x.%x.%x.base", baseLSN, baseTimeline, baseTs),
	}

	tr := tar.NewReader(r)

	for {
		th, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		fn := opts.Dir + th.Name
		if th.Typeflag != tar.TypeReg {
			if th.Typeflag == tar.TypeDir {
				os.Mkdir(fn, 0700)
			}
			continue
		}

		h, err := os.Create(fn)
		if err != nil {
			h.Close()
			log.Fatal(err)
		}
		_, err = io.Copy(h, tr)
		if err != nil {
			h.Close()
			log.Fatal(err)
		}
		h.Close()
	}

	ourPath, _ := filepath.Abs(os.Args[0])
	ioutil.WriteFile(opts.Dir+"recovery.conf", []byte(`
restore_command='`+ourPath+` restore_command "`+a.configFile+`" %f "%p"'
recovery_target='immediate'
recovery_target_timeline='latest'
recovery_target_action='shutdown'
`), 0700)

	dir, _ := filepath.Abs(opts.Dir)
	cmd := exec.Command("/usr/lib/postgresql/9.5/bin/postgres", "-D", dir, "-h", "", "-k", ".")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	os.Remove(opts.Dir + "recovery.conf")

	log.Print("recovered!")
}

func (a Agent) RestoreCommand(segment, to string) error {

	if strings.HasSuffix(segment, ".history") {
		return nil
	}

	var timeline, logical, physical uint64
	fmt.Sscanf(segment, "%08x%08x%08x", &timeline, &logical, &physical)
	if timeline == 0 {
		return errors.New("weirdLSN")
	}

	lsn := (logical << 32) | ((physical & 0xff) << 24)

	log.Print("segment=", segment, " lsn=", lsn)

	name := fmt.Sprintf("%012x.%x.wal", lsn, timeline)
	log.Print("pgbackup: get segment @", pgwal.LSN(lsn))

	r, err := a.store.Download(name)
	if err != nil {
		return err
	}

	f, err := os.Create(to)
	if err != nil {
		return err
	}

	_, err = io.Copy(f, r)
	if err != nil {
		return err
	}
	f.Close()

	//log.Print("restore timeline=", timeline, " lsn=", fmt.Sprintf("%x", lsn), " n=", n)
	return nil
}

type multiPartReader struct {
	Store Store
	Name  string
	n     int
	r     io.ReadCloser
}

func (mpr *multiPartReader) Read(d []byte) (int, error) {

	var o int
	for {
		if mpr.r != nil {
			n, err := mpr.r.Read(d)
			// XXX: .Close when appropriate?
			if err != io.EOF {
				return o + n, err
			}
			o += n
			d = d[n:]
		}
		if mpr.n == -1 {
			mpr.r = nil
			return o, io.EOF
		}
		// switch to next reader
		var err error
		mpr.r, err = mpr.Store.Download(fmt.Sprintf("%s.part%x", mpr.Name, mpr.n))
		mpr.n++
		if err != nil {
			// this was the last part
			mpr.r, err = mpr.Store.Download(mpr.Name)
			if err != nil {
				return o, err
			}
			mpr.n = -1
		}
	}
}
