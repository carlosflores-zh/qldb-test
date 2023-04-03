package metadata

// HistoryMetadata is the metadata of the document in QLDB (record from a table)
type HistoryMetadata struct {
	ID       string `ion:"id"`
	Version  int    `ion:"version"`
	DataHash []byte `ion:"dataHash"`
}

// Result is the result of the insert/update
type Result struct {
	DocumentID string `ion:"documentID"` // DocumentID, it is received after the insert/update
}

// ResponseHasDataRedaction is the response of the hasDataRedaction function
// used to validate if something has been redacted
type ResponseHasDataRedaction struct {
	CountHashes int `ion:"countHashes"`
}

// ImageAttestation represents the attestation document of an enclave image, in case we need it to register the enclaves?
type ImageAttestation struct {
	PCR1      string `ion:"pcr1"`
	PCR2      string `ion:"pcr2"`
	Signature string `ion:"signature"`
	Hash      string `ion:"hash"`
}
