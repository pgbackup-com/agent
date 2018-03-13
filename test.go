package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"./pg"
)

type TestOpts struct {
	Db       string
	User     string
	Query    string
	Target   string // "0/123456:5432"
	Timeline int
}

func (a Agent) Test(opts *TestOpts) error {

	dir, err := ioutil.TempDir("", "pgbackup-test")
	if err != nil {
		return err
	}

	a.Recover(&RecoverOpts{
		Timeline: opts.Timeline,
		Target:   opts.Target,
		Dir:      dir,
	})

	defer os.RemoveAll(dir)

	os.Remove(dir + "/recovery.conf")
	ioutil.WriteFile(dir+"/pg_hba.conf", []byte(`local all all trust`), 0700)

	cmd := exec.Command("/usr/lib/postgresql/9.5/bin/postgres", "-D", dir, "-h", "", "-k", ".", "-N", "8")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Start()

	for {
		conn, err := pg.NewConn(fmt.Sprintf("host=%s user=%s database=%s", dir, opts.User, opts.Db))
		if err != nil && strings.HasSuffix(err.Error(), "connect: no such file or directory") {
			time.Sleep(3 * time.Second)
			continue
		} else if err != nil {
			return err
		}
		defer conn.Close()
		r, err := conn.SimpleQuery(opts.Query)
		if err != nil {
			return err
		}

		for _, row := range r {
			var s string
			for _, v := range row {
				s += fmt.Sprintf("%v ", v)
			}
			os.Stdout.Write([]byte(s + "\n"))
		}
		break
	}

	cmd.Process.Signal(syscall.SIGINT)
	cmd.Wait()
	return nil
}
