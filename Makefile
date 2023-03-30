usage:      ## Show this help
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

install:    ## Install dependencies
	@which localstack || pip install localstack
	@which awslocal || pip install awscli-local

run-migrate:
	go run cmd/migrate/main.go us-east-2 ledger 2

run-app:
	go run cmd/test-app/main.go

run-transaction:
	go run cmd/transaction/main.go

run-delete:
	go run cmd/delete/main.go us-east-2 ledger

lint: ## Runs lint
	@if [[ -n "$(out)" ]]; then \
		mkdir -p $$(dirname "$(out)"); \
		golangci-lint run -v --out-format=checkstyle > "$(out)"; \
	else \
		golangci-lint run -v; \
	fi

start:
	@echo LOCALSTACK_API_KEY=$(LOCALSTACK_API_KEY)
	localstack start -d

create-ledger:
	awslocal qldb create-ledger --name test --permissions-mode ALLOW_ALL --endpoint-url=http://localhost:4566

stop:
	@echo
	localstack stop

logs:
	@localstack logs > logs.txt

test-ci:
	make start install ready run; return_code=`echo $$?`;\
	make logs; make stop; exit $$return_code;

diagrams:
	goplantuml -title DB -ignore metadata pkg/model/ > diagrams/classes.puml

.PHONY: usage install start run stop logs test-ci diagrams

