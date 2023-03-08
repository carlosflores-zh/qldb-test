QLDB test:
 - Requires localstack pro account
based on:
- https://github.com/localstack/localstack-pro-samples/tree/master/qldb-ledger-queries
- https://amazon-ion.github.io/ion-docs/
- https://github.com/localstack/localstack-pro-samples/tree/master/qldb-ledger-queries
- https://docs.aws.amazon.com/qldb/latest/developerguide/driver-quickstart-golang.html

Localstack:
- make install
- make start
- make create-ledger
- make run

AWS:
- set up aws env var credentials
- created ledger on aws console
- go run main.go