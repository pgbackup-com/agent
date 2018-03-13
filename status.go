package main

import (
	"fmt"
	"log"
	"time"

	"./pgwal"
)

func (a Agent) Status() {

	var res struct {
		Online bool   `json:"online"`
		TxID   int64  `json:"tx_id"`
		TxTs   int64  `json:"tx_ts"`
		TxLsn  uint64 `json:"tx_lsn"`

		FromLsn     uint64 `json:"from_lsn"`
		TillLsn     uint64 `json:"till_lsn"`
		WalUploadTs int64  `json:"wal_upload_ts"`
		WalLsn      uint64 `json:"wal_lsn"`
		WalSize     int64  `json:"wal_size"`
		BaseN       int64  `json:"base_n"`
		BaseSize    int64  `json:"base_size"`
	}

	a.BackendCall("GET", fmt.Sprintf("/v1/%d/status", a.BackupID), nil, &res)

	er := a.backendRequest("GET", fmt.Sprintf("/explorer/%d", a.BackupID), nil)

	log.Print()
	log.Print("         system id: ", a.GUID)
	log.Print("       pgbackup id: ", a.BackupID)
	log.Print()
	log.Print("       recoverable: ", pgwal.LSN(res.FromLsn), " - ", pgwal.LSN(res.TillLsn))
	log.Print("    wal upload lsn: ", pgwal.LSN(res.WalLsn))
	log.Print("   wal upload time: ", time.Since(time.Unix(0, res.WalUploadTs*1e6)).Truncate(time.Second))
	log.Print("  wal storage used: ", res.WalSize)
	log.Print(" base storage used: ", res.BaseSize)
	log.Print("      base backups: ", res.BaseN)
	log.Print()
	log.Print("          explorer: ", er.URL.String())
	log.Print("     stream online: ", res.Online)
	log.Print("        latest lsn: ", res.TxLsn)
	log.Print("         latest tx: ", res.TxID, " ", time.Since(time.Unix(0, res.TxTs*1e6)).Truncate(time.Second), " ago")
	log.Print()

	/*log.Print("      Tx at: ", txLsn, " timestamp: ", txTs)
	log.Print(" WAL backup: ", walLsn,
		"  lag: ", size(lsnDelta(txLsn, walLsn)),
		"  stored: ", walStoreTs)
	log.Print("Newest base: ", base1Lsn, ", ", base1Ts, ", ", size(base1Size))
	log.Print("Oldest base: ", base0Lsn, ", ", size(base1Size))
	log.Print("")
	log.Print(": ", base1Ts, ", ", base1Size)*/

}
