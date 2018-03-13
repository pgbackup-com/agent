package pgwal

import "encoding/binary"

type Endian struct {
	binary.ByteOrder
	WordSize uint32
	PageSize uint32
}

func (e Endian) AlignNext(offset uint32) uint32 {
	// MAXALIGN macro in postgres
	mask := e.WordSize - 1
	if offset&mask != 0 {
		offset = (offset & ^mask) + 8
	}
	return offset
}
