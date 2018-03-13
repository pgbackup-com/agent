package pgwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
)

type Page struct {
	Endian

	Magic      uint16
	Info       uint16
	Timeline   uint32
	LSN        LSN
	RemLen     uint32
	DataOffset uint32
	Data       []byte
}

var ErrWeirdPage = errors.New("weirdPage")

func ParsePage(d []byte) (*Page, error) {
	p := new(Page)

	if len(d) != 8192 {
		log.Print("short? ", len(d))
		return nil, ErrWeirdPage
	}
	p.PageSize = 8192

	if d[0] == 0xd0 && d[1] != 0xd0 {
		p.ByteOrder = binary.BigEndian
	} else if d[1] == 0xd0 {
		p.ByteOrder = binary.LittleEndian
	} else {
		log.Print(fmt.Sprintf("p.ByteOrder=%x %x", d[0], d[1]))
		return nil, ErrWeirdPage
	}

	p.WordSize = 8 // hmm, how to determine automatically?

	p.Magic = p.Uint16(d[0:2])
	if p.Magic != 0xd087 {
		log.Print(fmt.Sprintf("p.Magic=%x", p.Magic))
		return nil, ErrWeirdPage
	}
	p.Info = p.Uint16(d[2:4])
	p.Timeline = p.Uint32(d[4:8])
	p.LSN = LSN(p.Uint64(d[8:16]))
	p.RemLen = p.Uint32(d[16:20])
	p.DataOffset = p.AlignNext(20)
	if p.IsLong() {
		//systemID := p.Uint64(d[24:32])
		segmentSize := p.Uint32(d[32:36])
		blockSize := p.Uint32(d[36:40])
		if segmentSize != 0x1000000 || blockSize != 0x2000 {
			return nil, ErrWeirdPage
		}
		p.DataOffset = p.AlignNext(40)
	}
	p.Data = d[p.DataOffset:]

	return p, nil
}

func (p Page) IsCont() bool {
	return (p.Info & 1) == 1
}

func (p Page) IsLong() bool {
	return (p.Info & 2) == 2
}
