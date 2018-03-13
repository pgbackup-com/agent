package pgwal

type HeapData struct {
	Tablespace uint32
	Database   uint32
	Relation   uint32
	FromBlock  uint32
	FromOffset uint16
	ToBlock    uint32
	ToOffset   uint16
}
