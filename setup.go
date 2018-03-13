package main

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"./pg"
)

func (a *Agent) Setup() {

	if _, err := os.Stat("pgbackup.conf"); !os.IsNotExist(err) {
		log.Fatal("pgbackup.conf already exists\nDelete it if you want to create a new backup (deleting it may remove your private key and make existing backups unusable).")
	}

	var key [32]byte
	_, err := rand.Read(key[:])
	if err != nil {
		log.Fatal(err)
	}
	dbPass := fmt.Sprintf("%x", sha256.Sum256(key[:]))[:20]

	log.Print("pgbackup streams your database to encrypted, off-site storage.")
	log.Print()
	log.Print("To start, we need to connect to your postgresql database. We recommend using a local domain socket connection and adding a dedicated replication user:")
	log.Print()
	log.Print("- run query:", fmt.Sprintf("CREATE ROLE pgbackup REPLICATION LOGIN PASSWORD '%s';", dbPass))
	log.Print("- add to pg_hba.conf: local replication pgbackup password md5")
	log.Print("- run query: SELECT pg_reload_conf();")

	r := bufio.NewReader(os.Stdin)

	var conn *pg.Conn
	for {
		log.Print()

		fmt.Print("Socket path [/var/run/postgresql]: ")
		host := readLine(r, "/var/run/postgresql")

		a.ConnString = fmt.Sprintf("host=%s port=5432 user=pgbackup password=%s", host, dbPass)
		conn, err = pg.NewConn(a.ConnString + " replication=true")
		if err != nil && !strings.Contains(err.Error(), "max_wal_senders") {
			fmt.Print("Port [5432]: ")
			port := readLine(r, "5432")
			fmt.Print("User [pgbackup]: ")
			user := readLine(r, "pgbackup")
			fmt.Print("Password: ")
			pass := readLine(r, "")
			log.Print()
			a.ConnString = fmt.Sprintf("host=%s port=%s user=%s password=%s", host, port, user, pass)
			conn, err = pg.NewConn(a.ConnString + " replication=true")
		}
		if err == nil {
			break
		}

		if !strings.Contains(err.Error(), "max_wal_senders") {
			log.Print("Could not connect: ", err)
			log.Print()
			continue
		}

		log.Print()
		log.Print("Succesfully connected. However, your postgresql.conf needs some changes to use pgbackup.")

		for {
			log.Print("")
			log.Print("Please adjust the following in postgresql.conf:")
			log.Print("  max_wal_senders = 2 (or higher)")
			log.Print("  wal_level = 'archive' (or 'hot_standby')")
			log.Print("")
			log.Print("These changes require a hard restart of your database: pg_ctl restart")
			log.Print("")
			fmt.Print("Hit enter when ready")
			readLine(r, "")
			conn, err = pg.NewConn(a.ConnString)
			if err == nil {
				break
			}
			log.Print("Error: ", err)
			log.Print("")
		}
		break
	}

	defer conn.Close()

	systemID, _, _, err := conn.IdentifySystem()
	if err != nil {
		log.Fatal(err)
	}

	log.Print("Connected to postgres server", conn.ServerVersion, " system-id:", systemID)
	log.Print("")

	a.GUID = fmt.Sprintf("%d", systemID)

	log.Print("Please enter your email address so we can notify you in case of issues with your backup.")
	log.Print("")
	var email string
	for {
		fmt.Print("Email address: ")
		email = readLine(r, "")
		if email != "" {
			break
		}
	}

	log.Print("Creating pgbackup.com account...")
	_, err = rand.Read(key[:])
	if err != nil {
		log.Fatal(err)
	}
	a.Auth = fmt.Sprintf("%x", sha256.Sum256(key[:]))[:20]

	var res0 struct {
		ID    int    `json:"id"`
		Store string `json:"store"`
	}
	err = a.BackendCall("POST", "/v1/new", map[string]string{
		"email": email,
		"guid":  a.GUID,
		"store": "s3",
	}, &res0)
	if err != nil {
		log.Fatal(err)
	}

	a.Email = email
	a.BackupID = res0.ID
	a.Store = res0.Store

	// do more checks here

	log.Print("Creating encryption key...")

	_, err = rand.Read(key[:])
	if err != nil {
		log.Fatal(err)
	}
	a.EncryptKey = base64.StdEncoding.EncodeToString(key[:])

	// some sensible defaults:
	a.WarnAt = "wal:900"
	a.Retention = 720
	a.BaseInterval = 12
	a.Rollover = 300

	fh, err := os.Create("pgbackup.conf")
	if err != nil {
		log.Fatal(err)
	}
	defer fh.Close()
	e := json.NewEncoder(fh)
	e.SetIndent("", "\t")
	err = e.Encode(a)
	if err != nil {
		log.Fatal(err)
	}
	p, _ := filepath.Abs(fh.Name())
	log.Print("Created ", p)
	log.Print()
	log.Print("To start the agent as current user:")
	log.Print("  ", os.Args[0], " agent")
	log.Print()
	log.Print("To show backup status:")
	log.Print("  ", os.Args[0], " status")
	log.Print()
	log.Print("To test backup:")
	log.Print("  ", os.Args[0], " test")
	log.Print()
	log.Print("To install as system service:")
	log.Print("  sudo ", os.Args[0], " install")
	log.Print()

	er := a.backendRequest("GET", fmt.Sprintf("/explorer/%d", a.BackupID), nil)
	log.Print("Visit the explorer at: ", er.URL.String())
	log.Print()

	os.Exit(0)
}

func readLine(r *bufio.Reader, def string) string {
	v, _ := r.ReadString('\n')
	v = strings.Trim(v, " \n\t\t")
	if v == "" {
		v = def
	}
	return v
}
