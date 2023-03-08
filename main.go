package main

import (
	"context"
	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"
	log "github.com/sirupsen/logrus"
)

type Person struct {
	FirstName string `ion:"firstName"`
	LastName  string `ion:"lastName"`
	Age       int    `ion:"age"`
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}

	qldbSession := qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {
		options.Region = "us-east-2"
	})
	driver, err := qldbdriver.New(
		"test-ledger",
		qldbSession,
		func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		})
	if err != nil {
		panic(err)
	}

	defer driver.Shutdown(context.Background())

	// creates a transaction and executes statements
	_, err = driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		_, err := txn.Execute("CREATE TABLE People")
		if err != nil {
			log.Printf("Error creating table: %v", err)
			return nil, err
		}

		// When working with QLDB, it's recommended to create an index on fields we're filtering on.
		// This reduces the chance of OCC conflict exceptions with large datasets.
		result, err := txn.Execute("CREATE INDEX ON People (firstName)")
		if err != nil {
			log.Printf("Error creating index: %v", err)
			return nil, err
		}

		log.Printf("Result Create Index: %v", result)

		_, err = txn.Execute("CREATE INDEX ON People (age)")
		if err != nil {
			log.Printf("Error creating index: %v", err)
			return nil, err
		}

		log.Printf("Result Create Index: %v", result)

		return nil, nil
	})
	if err != nil {
		log.Errorf("Error creating table: %v", err)
	}

	_, err = driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("INSERT INTO People {'firstName': 'Jane', 'lastName': 'Doe', 'age': 77}")
	})
	if err != nil {
		panic(err)
	}

	person := Person{"John", "Doe", 54}

	_, err = driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("INSERT INTO People ?", person)
	})
	if err != nil {
		panic(err)
	}

	p, err := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT firstName, lastName, age FROM People WHERE age = 54")
		if err != nil {
			return nil, err
		}

		// Assume the result is not empty
		hasNext := result.Next(txn)
		if !hasNext && result.Err() != nil {
			return nil, result.Err()
		}

		ionBinary := result.GetCurrentData()

		temp := new(Person)
		err = ion.Unmarshal(ionBinary, temp)
		if err != nil {
			return nil, err
		}

		return *temp, nil
	})
	if err != nil {
		panic(err)
	}

	var returnedPerson Person
	returnedPerson = p.(Person)

	if returnedPerson != person {
		log.Println("Queried result does not match inserted struct")
	}

	person.Age += 10

	res, err := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("UPDATE People SET age = ? WHERE firstName = ?", person.Age, person.FirstName)
	})
	if err != nil {
		panic(err)
	}

	log.Printf("Result Update: %v", res)

	p, err = driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT firstName, lastName, age FROM People WHERE firstName = ?", person.FirstName)
		if err != nil {
			return nil, err
		}

		var people []Person
		for result.Next(txn) {
			ionBinary := result.GetCurrentData()

			temp := new(Person)
			err = ion.Unmarshal(ionBinary, temp)
			if err != nil {
				return nil, err
			}

			people = append(people, *temp)
		}
		if result.Err() != nil {
			return nil, result.Err()
		}

		return people, nil
	})
	if err != nil {
		panic(err)
	}

	var people []Person
	people = p.([]Person)

	updatedPerson := Person{"John", "Doe", 64}
	if people[0] != updatedPerson {
		log.Println("Queried result does not match updated struct")
	}

	log.Printf("Found People: %v", people)

	// don't delete the table, so we can run the example again
}
