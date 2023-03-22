package model

import "math/big"

// TransactionLog represents a transaction on any blockchain
type TransactionLog struct {
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
	PublicAddress string `ion:"publicAddress"`
	Type          string `ion:"type"`
}
