package model

// Share represents the shamir shard?
type Share struct {
	Id         string `ion:"id"`
	Signature1 string `ion:"signature1"`
	Owner      string `ion:"owner"`
	Material   string `ion:"material"`
	Status     string `ion:"status"`
}

// PrivateKey encrypted representation of  the private key
type PrivateKey struct {
	Note         string `ion:"note"`
	EncryptedKey string `ion:"encryptedKey"`
}
