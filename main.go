package main

import (
	"context"
	"crypto/ecdsa"
	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	log "github.com/sirupsen/logrus"
	"math/big"
	"os"
	"time"
)

const receiverAccount = "-"
const key = "-"
const accessID = "-"
const secretKey = "-"

type Transaction struct {
	TxID      string   `ion:"txID"`
	Nonce     uint64   `ion:"nonce"`
	GasFeeCap *big.Int `ion:"gasFeeCap"`
	Gas       uint64   `ion:"gas"`
	GasTipCap *big.Int `ion:"gasTipCap"`
	To        string   `ion:"to"`
	From      string   `ion:"from"`
	Value     *big.Int `ion:"value"`
	Data      []byte   `ion:"data"`
}

func main() {
	driver, err := connect()
	if err != nil {
		log.Errorf("error connecting/creating: %v", err)
	}

	defer driver.Shutdown(context.Background())

	for {
		tx, from, err := generateTx()
		if err != nil {
			log.Errorf("error generating tx: %v", err)
		}

		txDB := convert(tx, from)
		err = insertTx(driver, txDB)
		if err != nil {
			log.Errorf("error inserting tx: %v", err)
		}

		log.Printf("Inserted tx: %v", txDB.TxID)

		time.Sleep(5 * time.Second)
	}

}

func convert(tx *types.Transaction, from *common.Address) *Transaction {
	txDB := &Transaction{
		TxID:      tx.Hash().String(),
		Nonce:     tx.Nonce(),
		GasFeeCap: tx.GasFeeCap(),
		Gas:       tx.Gas(),
		GasTipCap: tx.GasTipCap(),
		To:        tx.To().String(),
		Value:     tx.Value(),
		Data:      tx.Data(),
		From:      from.String(),
	}

	return txDB
}

func insertTx(driver *qldbdriver.QLDBDriver, tx *Transaction) error {
	_, err := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("INSERT INTO transaction_chain ?", tx)
	})
	if err != nil {
		log.Printf("Error inserting struct: %v", err)
		return err
	}

	return nil
}

func connect() (driver *qldbdriver.QLDBDriver, err error) {
	os.Setenv("AWS_ACCESS_KEY_ID", accessID)
	os.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Printf("Error loading config: %v", err)
	}

	qldbSession := qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {
		options.Region = "us-east-2"
	})

	driver, err = qldbdriver.New(
		"ledger",
		qldbSession,
		func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		})
	if err != nil {
		log.Printf("Error creating driver: %v", err)
	}

	err = migrateQLDB(driver)
	if err != nil {
		log.Errorf("Error migrating QLDB: %v", err)
	}

	// For some reason Insert was failing after creating the tables, maybe it needs some time to propagate changes or something
	time.Sleep(2 * time.Second)

	return driver, nil
}

func migrateQLDB(driver *qldbdriver.QLDBDriver) error {
	// creates a transaction and executes statements
	_, err := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		_, err := txn.Execute("CREATE TABLE transaction_chain")
		if err != nil {
			log.Printf("Error creating table tx: %v", err)
		}

		log.Printf("Result Create Table Transaction")

		// When working with QLDB, it's recommended to create an index on fields we're filtering on.
		// This reduces the chance of OCC conflict exceptions with large datasets.
		_, err = txn.Execute("CREATE INDEX ON  transaction_chain (txID)")
		if err != nil {
			log.Printf("Error creating index: %v", err)
		}

		_, err = txn.Execute("CREATE INDEX ON transaction_chain (nonce)")
		if err != nil {
			log.Printf("Error creating index: %v", err)
		}

		_, err = txn.Execute("CREATE INDEX ON transaction_chain (to_address)")
		if err != nil {
			log.Printf("Error creating index: %v", err)
		}

		_, err = txn.Execute("CREATE TABLE transaction_signer")
		if err != nil {
			log.Printf("Error creating table: %v", err)
		}

		_, err = txn.Execute("CREATE INDEX ON transaction_signer (my_public_address)")
		if err != nil {
			log.Printf("Error creating index signer: %v", err)
		}

		return nil, nil
	})
	if err != nil {
		log.Errorf("Error creating tables: %v, MAYBE they were already created???", err)
	}

	return nil
}

func QueryTX(driver *qldbdriver.QLDBDriver, query string) (int, []Transaction, error) {
	p, err := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute(query)
		if err != nil {
			return nil, err
		}

		var txs []Transaction
		for result.Next(txn) {
			ionBinary := result.GetCurrentData()

			temp := new(Transaction)
			err = ion.Unmarshal(ionBinary, temp)
			if err != nil {
				return nil, err
			}

			txs = append(txs, *temp)
		}
		if result.Err() != nil {
			return nil, result.Err()
		}

		return txs, nil
	})
	if err != nil {
		return 0, nil, err
	}

	var people []Transaction
	people = p.([]Transaction)

	return len(people), people, err
}

func generateTx() (*types.Transaction, *common.Address, error) {
	client, err := ethclient.Dial("https://ava-testnet.public.blastapi.io/ext/bc/C/rpc")
	if err != nil {
		log.Fatal(err)
	}

	privateKey, err := crypto.HexToECDSA(key)
	if err != nil {
		log.Fatal(err)
	}

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		log.Fatal("error casting public key to ECDSA")
	}

	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)
	nonce, err := client.PendingNonceAt(context.Background(), fromAddress)
	if err != nil {
		log.Fatal(err)
	}

	toAddress := common.HexToAddress(receiverAccount)

	chainID, err := client.NetworkID(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	// Max Fee = (2 * Base Fee) + Max Priority Fee

	gasTipCap, err := client.SuggestGasTipCap(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	feeCap := big.NewInt(30000000000) // maxFeePerGas = 20 Gwei
	if err != nil {
		log.Fatal(err)
	}

	gasLimit := uint64(21000)
	txData := []byte("")
	balanceTransfer := big.NewInt(10000)

	newTx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     nonce,
		GasFeeCap: feeCap,
		Gas:       gasLimit,
		GasTipCap: gasTipCap,
		To:        &toAddress,
		Value:     balanceTransfer,
		Data:      txData,
	})

	signedTx, err := types.SignTx(newTx, types.LatestSignerForChainID(chainID), privateKey)
	if err != nil {
		log.Fatal(err)
	}

	err = client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		log.Errorf("Unable to submit transaction: %v", err)
	}

	log.Println("tx sent: ", signedTx.Hash().Hex())

	// Wait for the transaction to be confirmed
	<-waitTxConfirmed(context.Background(), client, signedTx.Hash())
	log.Println("tx confirmed: ", signedTx.Hash().Hex())
	return signedTx, &fromAddress, nil
}

// Returns a channel that blocks until the transaction is confirmed
func waitTxConfirmed(ctx context.Context, c *ethclient.Client, hash common.Hash) <-chan *types.Transaction {
	ch := make(chan *types.Transaction)
	go func() {
		for {
			tx, pending, _ := c.TransactionByHash(ctx, hash)
			if !pending {
				ch <- tx
			}

			time.Sleep(time.Millisecond * 500)
		}
	}()

	return ch
}
