package model

import (
	"math/big"
	"time"
)

// TODO: WIP all the structs in here represent tables in QLDB

type Migration struct {
	Version    int       `ion:"version"`
	MigratedAt time.Time `ion:"migratedAt"`
}

// Control represents the proposed control record table
// If the Document has both signatures, the DocumentID with the specified version in the table is considered good to be executed
// This table can actually validate any table, record and version (especially for admin changes)
type Control struct {
	ID              string          `ion:"id"`         // Document ID: same used to get history (unique)
	Signature1      []byte          `ion:"signature1"` // TODO: this is a byte slice, represents the signature of the document (hash)
	Signature2      []byte          `ion:"signature2"`
	Table           string          `ion:"table"`           // Table name of the table/record we are signing
	DocumentID      string          `ion:"documentId"`      // Document ID of the table/record we are signing
	Version         int             `ion:"version"`         // Version of the table/record we are signing
	ControlDocument ControlDocument `ion:"controlDocument"` // TODO: This is the document that actually needs to be signed by the admins
}

// ControlDocument is document to sign to insert in control
type ControlDocument struct {
	Table      string `ion:"table"`
	DocumentID string `ion:"documentId"` // Document ID of the table/record we are signing
	Version    int    `ion:"version"`
}

// Contract represents a whitelisted contract
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
// TODO: not sure how to represent this one
type Share struct {
	ID         string `ion:"id"` // Document ID: same used to get history (unique)
	Signature1 string `ion:"signature1"`
	Owner      string `ion:"owner"`
	Material   string `ion:"material"`
	Status     string `ion:"status"`
}

// PrivateKey encrypted representation of the private key, that can be decrypted by the enclaves
type PrivateKey struct {
	ID           string `ion:"id"` // Document ID: same used to get history (unique)
	Note         string `ion:"note"`
	EncryptedKey string `ion:"encryptedKey"`
}

// Image represents an enclave image, accepted and signed by the admins
// TODO: Can be signed in here or in Control table?
type Image struct {
	ID         string    `ion:"id"` // Document ID: same used to get history (unique)
	ImageID    string    `ion:"imageId"`
	Document   []byte    `ion:"document"` // TODO: this one should be something similar to the attestation document
	Signature1 []byte    `ion:"signature1"`
	Signature2 []byte    `ion:"signature2"`
	createdAt  time.Time `ion:"createdAt"`
	signedAt   time.Time `ion:"signedAt"`
}

// TransactionLog represents a transaction on any blockchain
// TODO: trying to make it as generic as possible, it will work as a log, doesn't need signatures
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
// TODO: just as a log, doesn't need signatures, or private info in here
type Signer struct {
	ID            string `ion:"id"` // Document ID: same used to get history (unique)
	PublicAddress string `ion:"publicAddress"`
	Type          string `ion:"type"`
	CreatedAt     string `ion:"createdAt"`
}

type Enclave struct {
	ID         string `ion:"id"`         // unique ID of the enclave
	Address    string `ion:"address"`    // network address of the enclave
	State      string `ion:"state"`      // state of the enclave (running, stopped, etc)
	Signature1 []byte `ion:"signature1"` // signatures of the admins that added this enclave
	Signature2 []byte `ion:"signature2"`
	Note       string `ion:"note"` // note of the admins that added this enclave
	CreatedAt  string `ion:"createdAt"`
	UpdatedAt  string `ion:"updatedAt"`
}
