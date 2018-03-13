package pgwal

/*type RecordScanner struct {
	R     io.Reader
	buf   []byte
	start LSN
	end   LSN

	lsn    LSN            // start of buf
	skip   int            // page header bytes we skipped
	e      Endian         // known after reading the first page
	page   [pageSize]byte // scratch space
	pageAt int
}*/

/*
func NewRecordScanner(r io.Reader) *RecordScanner {
	return &RecordScanner{r: r}
}

func (s *RecordScanner) Next() (*Record, error) {
	for {
		if s.buf != nil {
			r, err := ParseRecord(s.buf, s.e, s.lsn)
			if err == nil && r.Rmgr == 0x00 && r.Info == 0x40 {
				// SWITCH record


			} else if err == nil {
				log.Print("parsed record=", r.Len, " lsn=", s.lsn, " rmgr=", r.Rmgr, " info=", r.Info)
				skip := s.e.AlignNext(r.Len)
				//r.LSN = s.lsn
				s.lsn += LSN(skip) + LSN(s.skip)
				s.skip = 0
				s.buf = s.buf[skip:]
				return r, nil

			} else if err != errIncomplete {
				log.Print("parsed rec err=", err, " lsn=", s.lsn)
				return nil, err
			}
		}

		log.Print("READER READ A")

		n, err := s.r.Read(s.page[s.pageAt:])
		log.Print("  READ ", n)
		s.pageAt += n
		if err != nil {
			log.Print("READER READ e ", err)
			return nil, err
		}

		if s.pageAt != pageSize {
			log.Print("EOF")
			return nil, io.EOF
		}

		s.pageAt = 0

		page, err := ParsePage(s.page[:])
		if err != nil {
			return nil, err
		}
		if s.e.ByteOrder == nil {
			s.e = page.Endian
		}

		if s.buf == nil {
			skip := s.e.AlignNext(page.RemLen)
			if int(skip) >= len(page.Data) {
				// skip complete page
				continue
			}

			log.Print("new buf skip=", skip)

			s.buf = append([]byte{}, page.Data[skip:]...)
			s.lsn = page.LSN + LSN(page.DataOffset) + LSN(skip)

			log.Print("s.buf=", len(s.buf))

		} else {
			if (page.RemLen == 0) != (len(s.buf) == 0) {
				// this shouldn't happen
				//log.Fatal("weird3, lsn=", s.lsn, " remlen=", page.RemLen, " buf=", len(s.buf))
			}
			s.buf = append(s.buf, page.Data...)
			s.skip += int(page.DataOffset)
		}
	}
}
*/
