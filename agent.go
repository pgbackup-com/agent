package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"./pg"
	"./pgwal"
)

func (a *Agent) Agent() error {

	for {
		a.pgb = &http.Client{}
		a.exitC = make(chan bool)
		a.uploadC = make(chan *Upload, 16)
		a.txLogC = make(chan []byte, 16)

		wc := make(chan bool)
		go run("txlog", a.TxSender, wc)
		go run("upload", a.Uploader, wc)
		go run("pump", a.Pump, wc)
		<-wc
		close(a.exitC)
		log.Fatal("bye")
		<-wc
		<-wc
		time.Sleep(2 * time.Second)
	}
}

func run(name string, f func() error, wc chan bool) {
	defer func() {
		if rvr := recover(); rvr != nil {
			log.Print(name, ": ", rvr, " (panic)")
			debug.PrintStack()
		}
		wc <- true
	}()

	err := f()
	if err != nil {
		log.Print(name, ": ", err)
	}
}

const (
	walSegmentSize  = 0x1000000  // 16MB, compressed ~5MB
	baseSegmentSize = 0x10000000 // 256MB, compressed ~50MB
)

func (a *Agent) Pump() error {
	// main backup routine

	err := a.BackendCall("PUT", fmt.Sprintf("/v1/%d", a.BackupID), map[string]interface{}{
		"email":         a.Email,
		"warn_at":       a.WarnAt,
		"retention":     a.Retention,
		"base_interval": a.BaseInterval,
		"rollover":      a.Rollover,
	}, nil)
	if err != nil {
		return err
	}

	walConn, err := pg.NewConn(a.ConnString + " replication=true")
	if err != nil {
		return err
	}
	defer walConn.Close()

	baseConn, err := pg.NewConn(a.ConnString + " replication=true")
	if err != nil {
		return err
	}
	defer baseConn.Close()

	var forceNewBase bool

restart:

	systemID, timeline, dbLsn, err := walConn.IdentifySystem()
	if err != nil {
		return err
	}

	if fmt.Sprintf("%d", systemID) != a.GUID {
		return fmt.Errorf("systemID mismatch; known=%s database=%d", a.GUID, systemID)
	}

	var walLsn uint64      // next wal lsn
	var baseLsn uint64     // last base lsn
	var baseTime time.Time // last base time
	if !forceNewBase {
		// scan files and find latest wal position and base backup
		files, err := a.store.List()
		if err != nil {
			return err
		}

		for _, f := range files {
			var lsn0 uint64
			var timeline0 int
			var time0 uint64
			fmt.Sscanf(f.Name, "%012x.%x.%x.", &lsn0, &timeline0, &time0)
			if strings.HasSuffix(f.Name, ".wal") && timeline0 != 0 && timeline0 <= timeline {
				walLsn = lsn0 + walSegmentSize
			} else if strings.HasSuffix(f.Name, ".base") && time0 != 0 && timeline0 <= timeline {
				baseLsn = lsn0
				baseTime = time.Unix(int64(time0), 0)
			}
		}
	}

	var baseC <-chan []byte
	var basePart int
	if baseLsn == 0 || walLsn == 0 {
		_, lsn0, c, err := baseConn.BaseBackup("pgbackup", 0)
		if err != nil {
			return err
		}
		lsn1, err := pgwal.ParseLSN(lsn0)
		if err != nil {
			return err
		}

		baseLsn = uint64(lsn1)
		walLsn = baseLsn & ^uint64(walSegmentSize-1)
		baseC = c
		baseTime = time.Now().UTC()

		log.Print("newBackup base:", pgwal.LSN(baseLsn), "  wal:", pgwal.LSN(walLsn), "  server:", dbLsn, "  system:", systemID)

	} else {
		log.Print("continue wal:", pgwal.LSN(walLsn), "  lastBase:", pgwal.LSN(baseLsn), " (", time.Since(baseTime).Truncate(time.Second), " ago)  server:", dbLsn, "  system:", systemID)
	}

	walC, err := walConn.StartReplication(pgwal.LSN(walLsn).String(), timeline)
	if err != nil {
		return err
	}

	var walBuf, baseBuf []byte // pieces to upload

	var rolloverT <-chan time.Time
	if a.Rollover > 0 {
		rolloverT = time.After(time.Duration(a.Rollover) * time.Second)
	}

	for {
		var upload *Upload
		select {
		case <-a.exitC:
			return nil

		case d := <-walC:
			if d.Data == nil {
				// indicates wal part is no longer available
				forceNewBase = true
				log.Print("missingWal forcing new backup")
				goto restart
			}

			forceNewBase = false

			if d.Lsn != walLsn+uint64(len(walBuf)) {
				return fmt.Errorf("weirdNextLsn=%d expected=%d+%d", d.Lsn, walLsn, len(walBuf))
			}

			walBuf = append(walBuf, d.Data...)
			if len(walBuf) >= walSegmentSize {
				upload = &Upload{
					Name: fmt.Sprintf("%012x.%x.wal", walLsn, timeline),
					Body: bytes.NewReader(walBuf[:walSegmentSize]),
				}
				walLsn += uint64(walSegmentSize)
				walBuf = walBuf[walSegmentSize:]
				if a.Rollover > 0 {
					rolloverT = time.After(time.Duration(a.Rollover) * time.Second)
				}
			}

			select {
			case <-a.exitC:
				return nil
			case a.txLogC <- d.Data:
			}

		case d := <-baseC:
			if d == nil {
				upload = &Upload{
					Name: fmt.Sprintf("%012x.%x.%x.base", baseLsn, timeline, baseTime.Unix()),
					Body: bytes.NewReader(baseBuf),
				}
				basePart++
				log.Print("baseBackupDone chunks:", basePart)
				baseC = nil
				baseBuf = nil
				basePart = 0
				// keep baseTime as "time of last base backup"
				break
			}
			baseBuf = append(baseBuf, d...)
			if len(baseBuf) >= baseSegmentSize {
				upload = &Upload{
					Name: fmt.Sprintf("%012x.%x.%x.base.part%x", baseLsn, timeline, baseTime.Unix(), basePart),
					Body: bytes.NewReader(baseBuf[:baseSegmentSize]),
				}
				baseBuf = baseBuf[baseSegmentSize:]
				basePart++
			}

		case <-rolloverT:
			if baseC == nil {
				// Hmm, it would be nicer if we could somehow trigger switch using the
				// baseConn. Perhaps issue a new base backup and cancel it right away?
				rolloverConn, err := pg.NewConn(a.ConnString)
				if rolloverConn != nil {
					rolloverConn.SimpleQuery("select pg_switch_xlog()")
					log.Print("xlog-rollover ", a.Rollover, "s after last segment")
					rolloverConn.Close()
				} else {
					log.Print("xlog-rollover: failed, err=", err)
				}
			}
		}
		if upload != nil {
			select {
			case <-a.exitC:
				return nil
			case a.uploadC <- upload:
			}
		}

		if baseC == nil && (time.Since(baseTime) > 4*time.Hour) {
			_, lsn0, c, err := baseConn.BaseBackup("pgbackup", 0)
			if err != nil {
				return err
			}
			lsn1, err := pgwal.ParseLSN(lsn0)
			if err != nil {
				return err
			}
			baseLsn = uint64(lsn1)
			baseC = c
			baseTime = time.Now().UTC()
			log.Print("baseBackup@", pgwal.LSN(baseLsn), " at ", baseTime)
			rolloverT = nil // reset rollover timer
		}
	}
}
