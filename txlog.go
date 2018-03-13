package main

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"./pgwal"
)

func (a Agent) TxSender() error {

	lens := make(map[uint32]int)
	//age := make(map[uint32]int)
	out := &bytes.Buffer{}
	var lastLsn uint64
	var lastTx uint32
	var firstTs, lastTs int64

	flushT := time.After(10 * time.Second) // flush stats based on wall clock

	var buf []byte
	var cont pgwal.RecordCont

	flush := func() {
		err := a.BackendCall("POST", fmt.Sprintf("/v1/%d/tx", a.BackupID), map[string]string{"d": out.String()}, nil)
		if err != nil {
			log.Print("txlog: err=", err)
		}
		lastLsn = 0
		lastTx = 0
		lastTs = 0
		firstTs = 0
		lens = make(map[uint32]int)
		out = &bytes.Buffer{}
		flushT = time.After(60 * time.Second)
	}

	for {
		select {
		case <-a.exitC:
			return nil
		case d := <-a.txLogC:
			buf = append(buf, d...)
			for len(buf) > 8192 {
				p, err := pgwal.ParsePage(buf[:8192])
				if err != nil {
					log.Print("txstream: could not parse page: ", err)
					buf = nil
					cont = pgwal.RecordCont{}
					break
				}
				buf = buf[8192:]
				for _, r := range p.Records(&cont) {
					if r.TxID == 0 {
						continue
					}
					lens[r.TxID] += int(r.Len)
					if r.Type() == "commit" {
						//log.Print("Commit! ", r.TxID)
						ct := r.CommitTime()
						ts := ct.UnixNano() / int64(time.Millisecond)
						l := lens[r.TxID]
						// lsn txid commitTime len
						out.WriteString(fmt.Sprintf("%x %x %x %x\n",
							uint64(r.LSN)-lastLsn,
							r.TxID-lastTx,
							ts-lastTs,
							l,
						))
						lastLsn = uint64(r.LSN)
						lastTx = r.TxID
						lastTs = ts
						if firstTs == 0 {
							firstTs = ts
						}
						delete(lens, r.TxID)
						//delete(ages, r.TxID)
					}
				}
				if firstTs > 0 && time.Duration(lastTs-firstTs)*time.Millisecond > 60*time.Second {
					flush()
				}
			}

		case <-flushT:
			flush()
		}
	}
}
