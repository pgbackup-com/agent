package main

import (
	"bytes"
	"crypto/aes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

var (
	Version string
	Host    = "pgbackup.com"
)

type Agent struct {
	EncryptKey   string `json:"encrypt-key"`
	ConnString   string `json:"conn-string"`
	Auth         string `json:"auth"`
	BackupID     int    `json:"id"`
	GUID         string `json:"guid"`
	Store        string `json:"store"`
	Email        string `json:"email"`
	WarnAt       string `json:"warn-at"`
	Retention    int    `json:"retention"`
	BaseInterval int    `json:"base-interval"`
	Rollover     int    `json:"rollover"`

	store Store
	pgb   *http.Client

	exitC      chan bool
	txLogC     chan []byte
	uploadC    chan *Upload
	configFile string
}

func main() {
	log.SetFlags(0)

	var a Agent

	a.pgb = &http.Client{}

	var cmd string
	if len(os.Args) >= 2 {
		cmd = os.Args[1]
	}

	if cmd == "setup" {
		a.Setup()

	} else if cmd == "install" {
		a.Install()

	} else if cmd == "agent" {
		a.ReadConfig()
		a.Agent()

	} else if cmd == "status" {
		a.ReadConfig()
		a.Status()

	} else if cmd == "recover" {
		opts := &RecoverOpts{}
		f := flag.NewFlagSet("recover", flag.ExitOnError)
		f.StringVar(&opts.Target, "target", "latest", "Target to restore; 'latest' or [lsn]:[txid] or [lsn]:[txid]:[timeline]")
		f.StringVar(&opts.Dir, "dir", "", "Directory where to load recovered cluster")
		f.Parse(os.Args[2:])
		if opts.Dir == "" {
			f.PrintDefaults()
			os.Exit(2)
		}
		a.ReadConfig()
		a.Recover(opts)

	} else if cmd == "query" {
		opts := &QueryOpts{}
		f := flag.NewFlagSet("query", flag.ExitOnError)
		f.StringVar(&opts.Target, "target", "latest", "Target to restore; 'latest' or [lsn]:[txid] or [lsn]:[txid]:[timeline]")
		f.StringVar(&opts.Db, "db", "", "Database to run query on")
		f.StringVar(&opts.User, "user", "", "User to run query as")
		f.StringVar(&opts.Query, "query", "", "Query to run")
		f.Parse(os.Args[2:])
		if opts.Db == "" || opts.User == "" || opts.Query == "" {
			f.PrintDefaults()
			os.Exit(2)
		}
		a.ReadConfig()
		a.Query(opts)

	} else if cmd == "restore_command" {
		a.readConfig(os.Args[2])
		a.RestoreCommand(os.Args[3], os.Args[4])

	} else {
		log.Fatal("usage: pgbackup [setup|install|agent|status|recover|query|dumptable]")
	}
}

func (a *Agent) ReadConfig() {
	err := a.readConfig("pgbackup.conf")
	if err != nil {
		err = a.readConfig("/etc/pgbackup.conf")
		if err != nil {
			log.Fatal("Could not read pgbackup.conf or /etc/pgbackup.conf. Use '", os.Args[0], " setup' to start a new backup.")
		}
	}
}

func (a *Agent) readConfig(file string) error {

	fh, err := os.Open(file)
	if err != nil {
		return err
	}

	a.configFile, _ = filepath.Abs(fh.Name())

	json.NewDecoder(fh).Decode(a)
	if a.EncryptKey == "" || a.ConnString == "" || a.Store == "" || a.GUID == "" {
		return errors.New("could not parse pgbackup.conf")
	}

	store, err := NewStore(a.Store)
	if err != nil {
		return err
	}

	key, err := base64.StdEncoding.DecodeString(a.EncryptKey)
	if err != nil {
		return err
	}

	aes, err := aes.NewCipher(key)
	if err != nil {
		return err
	}

	a.store = &cryptStore{Store: store, Aes: aes}

	return nil
}

func (a *Agent) BackendCall(method, path string, data interface{}, result interface{}) error {

	var body io.Reader
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			return err
		}
		body = bytes.NewBuffer(b)
	}
	req := a.backendRequest(method, path, body)
	if data != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := a.pgb.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("status:%d", resp.StatusCode)
	}

	if result != nil {
		err := json.NewDecoder(resp.Body).Decode(&result)
		if err != nil {
			return err
		}
	} else {
		io.Copy(ioutil.Discard, resp.Body)
	}
	resp.Body.Close()

	return nil
}

func (a Agent) backendRequest(method, path string, body io.Reader) *http.Request {
	req, err := http.NewRequest(method, fmt.Sprintf("https://%s%s?auth=%s", Host, path, a.Auth), body)
	if err != nil {
		panic(err)
	}
	return req
}
