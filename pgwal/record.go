package pgwal

import (
	"fmt"
	"log"
	"time"
)

type Record struct {
	Endian

	LSN  LSN    // added later, not part of the data
	Len  uint32 // "total_len" field in postgres
	TxID uint32
	Prev LSN
	Info byte
	Rmgr byte
	CRC  uint32
	Data []byte
}

type RecordCont struct {
	buf  []byte
	lsn  LSN // lsn at start of buf
	skip int // header bytes skipped while filling buf
}

func (p *Page) Records(cont *RecordCont) []*Record {

	buf := cont.buf
	lsn := cont.lsn
	skip := cont.skip
	if len(buf) == 0 {
		if int(p.RemLen) > len(p.Data) {
			return nil
		}
		//log.Print("page @", p.LSN, " skip remlen=", p.RemLen)
		o := p.AlignNext(p.RemLen)
		buf = p.Data[o:]
		lsn = p.LSN + LSN(p.DataOffset) + LSN(o)
		skip = 0
	} else {
		// assert p.RemLen > 0
		if p.RemLen == 0 {
			log.Print("page @", p.LSN, ": we still had ", len(buf), " continuation bytes, but next page RemLen=0")
		}
		buf = append(buf, p.Data...)
		skip += int(p.DataOffset)
	}

	//log.Print("records @", p.LSN)

	rr := []*Record{}
	for len(buf) >= 24 {
		r := new(Record)
		r.Len = p.Uint32(buf[0:4])
		//log.Print("recordLen=", r.Len)
		if r.Len < 24 || r.Len > 0x1000000 {
			if r.Len != 0 {
				// this is weird
				log.Print("weirdRecord@", lsn, " len=", r.Len)
			} // else: an empty page remainder, should happen only after SWITCH records
			buf = nil
			lsn = LSN(0)
			break
		} else if int(r.Len) > len(buf) {
			break
		}
		r.Endian = p.Endian
		r.LSN = lsn
		r.TxID = p.Uint32(buf[4:8])
		r.Prev = LSN(p.Uint64(buf[8:16]))
		r.Info = buf[16]
		r.Rmgr = buf[17]
		r.CRC = p.Uint32(buf[20:24])
		r.Data = buf[24:]
		rr = append(rr, r)

		//log.Print("  record lsn=", lsn, " len=", r.Len, " prevlsn=", r.Prev, " type=", r.Type(), " txid=", r.TxID)
		offset := p.AlignNext(r.Len)
		buf = buf[offset:]
		lsn += LSN(offset) + LSN(skip)
		skip = 0
	}

	cont.buf = buf
	cont.lsn = lsn
	cont.skip = skip
	return rr
}

func (r Record) IsInit() bool {
	return (r.Info & 0x80) == 0x80
}

const (
	RmgrXlog  = 0x00
	RmgrTx    = 0x01
	RmgrHeap2 = 0x09
	RmgrHeap  = 0x0A
	RmgrBtree = 0x0B
)

func (r Record) Type() string {
	t := (uint16(r.Rmgr) << 8) | (uint16(r.Info) & 0x70)
	switch t {
	case 0x100, 0x160:
		return "commit"
	case 0x120:
		return "abort"
	case 0x950:
		return "multi_insert"
	case 0xa00:
		return "insert"
	case 0xa70:
		return "inplace"
	case 0xa10:
		return "delete"
	case 0xa20:
		return "update"
	case 0xa40:
		return "hot_update"
	case 0x910:
		return "heap2:clean"
	case 0xa60:
		return "heap:lock"
	case 0xb00:
		return "btree:insert_leaf"
	}
	return fmt.Sprintf("unknown:%04x", t)
}

var pgEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

func (r Record) Rel() (tblspcID, dbID, relID uint32) {
	// Different blocks in this record might be for different relations, but let's use
	// this heuristic for now
	r.eachBlock(func(a, b, c uint32, d []byte) {
		if a != 0 {
			tblspcID, dbID, relID = a, b, c
		}
	})
	return
}

func (r Record) CommitTime() time.Time {
	if r.Type() == "commit" {
		main := r.eachBlock(nil)
		if len(main) > 8 {
			ts := r.Uint64(main[0:8])
			return pgEpoch.Add(time.Duration(ts) * time.Microsecond)
		}
	}
	return time.Time{}
}

func (r Record) eachBlock(cb func(uint32, uint32, uint32, []byte)) []byte {

	data := r.Data

	var tblspcID, dbID, relID uint32
	for {
		if len(data) < 5 {
			log.Print("shit weird block header=", len(data), " ", data)
			return []byte{}
		}

		blockID := data[0]
		// 0-32: indicates a XLogRecordBlockHeader
		if blockID == 254 {
			// 254: XLogRecordDataHeaderShort
			data = data[5:]
			break
		} else if blockID == 255 {
			// 255: XLogRecordDataHeaderLong
			data = data[2:]
			break
		}
		if len(data) < 24 {
			log.Print("shit weird block header=", len(data), " ", data)
			return []byte{}
		}

		forkFlags := data[1]
		//length := r.Uint16(data[2:4])
		data = data[4:]

		if (forkFlags & 0x10) == 0x10 {
			// BKPBLOCK_HAS_IMAGE
			// XLogRecordBlockImageHeader
			// uint16 length
			// uint16 hole_offset
			// uint8 bimg_info
			// length = r.Uint16(data[0:2])
			bimgInfo := data[4]
			data = data[5:]
			if (bimgInfo & 0x03) == 0x03 {
				// BKPIMAGE_HAS_HOLE & BKPIMAGE_IS_COMPRESSED
				// XLogRecordBlockCompressHeader
				// uint16 hole_length
				data = data[2:]
			}
		}

		if (forkFlags & 0x80) == 0 {
			// not BKPBLOCK_SAME_REL
			tblspcID, dbID, relID = r.Uint32(data[0:4]), r.Uint32(data[4:8]), r.Uint32(data[8:12])
			data = data[12:]
		}

		//blockNumber := r.Uint32(data[0:4])
		data = data[4:]
		if cb != nil {
			cb(tblspcID, dbID, relID, data)
		}
	}
	return data
}
