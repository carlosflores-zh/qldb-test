package model

// Control represents the proposed control record table
type Control struct {
	Id         string `ion:"id"`
	Signature1 string `ion:"signature1"`
	Signature2 string `ion:"signature2"`
	Table      string `ion:"table"`
	TxID       string `ion:"txID"`
	Version    string `ion:"version"`
	CreatedBy  string `ion:"createdBy"`
}

type Contract struct {
	Id        string `ion:"id"`
	Address   string `ion:"address"`
	Input     string `ion:"input"`
	Output    string `ion:"output"`
	Network   string `ion:"network"`
	SendFunds bool   `ion:"sendFunds"`
	Execution bool   `ion:"execution"`
}
