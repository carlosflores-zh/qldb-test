package model

import (
	"math/big"
	"time"
)

// TODO: WIP all the structs in here represent tables in QLDB

type Migration struct {
	Version   int       `ion:"version"`
	UpdatedAt time.Time `ion:"updatedAt"`
	Active    bool      `ion:"active"`
}

// Control represents the proposed control record table
// If the Document has both signatures, the DocumentID with the specified version in the table is considered good to be executed
type Control struct {
	ID         string `ion:"id"` // Document ID: same used to get history (unique)
	Signature1 string `ion:"signature1"`
	Signature2 string `ion:"signature2"`
	Table      string `ion:"table"`
	DocumentID string `ion:"documentId"` // Document ID of the table/record we are signing
	Version    int    `ion:"version"`    // Version of the table/record we are signing
}

type Contract struct {
	ID        string `ion:"id"` // Document ID: same used to get history (unique)
	Address   string `ion:"address"`
	Input     string `ion:"input"`
	Output    string `ion:"output"`
	Network   string `ion:"network"`
	SendFunds bool   `ion:"sendFunds"`
	Execution bool   `ion:"execution"`
}

// Share represents the shamir shard?
type Share struct {
	ID         string `ion:"id"` // Document ID: same used to get history (unique)
	Signature1 string `ion:"signature1"`
	Owner      string `ion:"owner"`
	Material   string `ion:"material"`
	Status     string `ion:"status"`
}

// PrivateKey encrypted representation of  the private key
type PrivateKey struct {
	ID           string `ion:"id"` // Document ID: same used to get history (unique)
	Note         string `ion:"note"`
	EncryptedKey string `ion:"encryptedKey"`
}

type Image struct {
	ID         string `ion:"id"` // Document ID: same used to get history (unique)
	ImageID    string `ion:"imageId"`
	Document   []byte `ion:"document"`
	Signature1 string `ion:"signature1"`
	Signature2 string `ion:"signature2"`
}

// TransactionLog represents a transaction on any blockchain
type TransactionLog struct {
	ID    string   `ion:"id"` // Document ID: same used to get history (unique)
	TxID  string   `ion:"txID"`
	Nonce uint64   `ion:"nonce"`
	Fee   *big.Int `ion:"Fee"`
	To    string   `ion:"to"`
	From  string   `ion:"from"`
	Value *big.Int `ion:"value"`
	Data  []byte   `ion:"data"`
	Block uint64   `ion:"block"`
}

// Signer represents a signer on any blockchain
type Signer struct {
	ID            string `ion:"id"` // Document ID: same used to get history (unique)
	PublicAddress string `ion:"publicAddress"`
	Type          string `ion:"type"`
}
