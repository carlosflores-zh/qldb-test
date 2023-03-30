package metadata

type HistoryMetadata struct {
	ID       string `ion:"id"`
	Version  int    `ion:"version"`
	DataHash []byte `ion:"dataHash"`
}

type Result struct {
	DocumentID string `ion:"documentID"` // DocumentID, it is received after the insert/update
}

type ResponseHasDataRedaction struct {
	CountHashes int `ion:"countHashes"`
}

type ImageAttestation struct {
	PCR1      string `ion:"pcr1"`
	PCR2      string `ion:"pcr2"`
	Signature string `ion:"signature"`
	Hash      string `ion:"hash"`
}
