package main

import (
	"context"
	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

type Person struct {
	FirstName       string `ion:"firstName"`
	LastName        string `ion:"lastName"`
	Age             int    `ion:"age"`
	FavouriteColour string `ion:"favouriteColour"`
	Allergies       string `ion:"allergies"`
	Title           string `ion:"title"`
	Dimension       string `ion:"dimension"`
}

func main() {
	os.Setenv("AWS_ACCESS_KEY_ID", "-")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "-")

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Printf("Error loading config: %v", err)
	}

	qldbSession := qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {
		options.Region = "us-east-2"
	})

	driver, err := qldbdriver.New(
		"ledger",
		qldbSession,
		func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		})
	if err != nil {
		log.Printf("Error creating driver: %v", err)
	}

	defer driver.Shutdown(context.Background())

	// creates a transaction and executes statements
	_, err = driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		// -- Create a table People --
		_, err := txn.Execute("CREATE TABLE People")
		if err != nil {
			log.Printf("Error creating table: %v", err)
			return nil, err
		}

		log.Printf("Result Create Table People")

		// When working with QLDB, it's recommended to create an index on fields we're filtering on.
		// This reduces the chance of OCC conflict exceptions with large datasets.
		_, err = txn.Execute("CREATE INDEX ON People (firstName)")
		if err != nil {
			log.Printf("Error creating index: %v", err)
		}

		log.Printf("Result Create Index People firstname")

		_, err = txn.Execute("CREATE INDEX ON People (age)")
		if err != nil {
			log.Printf("Error creating index: %v", err)
		}

		log.Printf("Result Create Index People age")

		_, err = txn.Execute("CREATE INDEX ON People (title)")
		if err != nil {
			log.Printf("Error creating index: %v", err)
		}

		log.Printf("Result Create Index People title")

		_, err = txn.Execute("CREATE INDEX ON People (dimension)")
		if err != nil {
			log.Printf("Error creating index: %v", err)
		}

		log.Printf("Result Create Index People dimension")

		_, err = txn.Execute("CREATE TABLE VehicleRegistration")
		if err != nil {
			log.Printf("Error creating table: %v", err)
		}

		log.Printf("Result Create Table VehicleRegistration")

		_, err = txn.Execute("CREATE TABLE Vehicle")
		if err != nil {
			log.Printf("Error creating table: %v", err)
		}

		log.Printf("Result Create Table Vehicle")

		// When working with QLDB, it's recommended to create an index on fields we're filtering on.
		// This reduces the chance of OCC conflict exceptions with large datasets.
		_, err = txn.Execute("CREATE INDEX ON VehicleRegistration (VIN)")
		if err != nil {
			log.Printf("Error creating index: %v", err)
		}

		log.Printf("Result Create Index VehicleRegistration")

		_, err = txn.Execute("CREATE INDEX ON VehicleRegistration (LicensePlateNumber)")
		if err != nil {
			log.Printf("Error creating index: %v", err)
		}

		log.Printf("Result Create Index VehicleRegistration 2")

		_, err = txn.Execute("CREATE INDEX ON Vehicle (VIN)")
		if err != nil {
			log.Printf("Error creating index: %v", err)
		}

		log.Printf("Result Create Index Vehicle VIN")

		return nil, nil
	})
	if err != nil {
		log.Errorf("Error creating tables: %v, MAYBE they were already created???", err)
	}

	person := Person{"John", "Doe", 54, "Blue", "Peanuts", "Mr", "Earth"}

	// For some reason Insert was failing after creating the tables, maybe it needs some time to propagate changes or something
	time.Sleep(2 * time.Second)

	_, err = driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("INSERT INTO People ?", person)
	})
	if err != nil {
		log.Printf("Error inserting struct: %v", err)
	}

	p, err := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT firstName, lastName, age, favouriteColour, allergies, title, dimension FROM People WHERE age = 54")
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
		log.Printf("Error querying table (one person): %v", err)
	}

	var returnedPerson Person
	returnedPerson = p.(Person)

	if returnedPerson != person {
		log.Println("Queried result does not match inserted struct, expected: ", person, " but got: ", returnedPerson)
	}

	person.Age += 10

	res, err := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute("UPDATE People SET age = ? WHERE firstName = ?", person.Age, person.FirstName)
	})
	if err != nil {
		log.Printf("Error updating struct: %v", err)
	}

	log.Printf("Result Update: %v", res)

	i, people, err := queryPeople(driver, "SELECT firstName, lastName, age FROM People")
	if err != nil {
		log.Printf("Error querying table (all people): %v", err)
	}
	log.Printf("Result Query: %v - count: %d", people, i)

	if i > 5 {
		log.Printf("trying to create a new index")
		_, err = driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
			return txn.Execute("CREATE INDEX ON People (lastName)")
		})
		if err != nil {
			log.Printf("Error creating new index: %v", err)
		}
	}

	i, people, err = queryPeople(driver, "SELECT firstName, lastName, age FROM People LIMIT 1")
	if err != nil {
		log.Errorf("expected error (LIMIT & ORDER not supported) %v", err)
	}
}

func queryPeople(driver *qldbdriver.QLDBDriver, query string) (int, []Person, error) {
	p, err := driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute(query)
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
		log.Fatalf("Error querying people: %v", err)
	}

	var people []Person
	people = p.([]Person)

	return len(people), people, err
}
