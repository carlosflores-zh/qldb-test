export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test
export AWS_DEFAULT_REGION = us-east-1

usage:      ## Show this help
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

install:    ## Install dependencies
	@which localstack || pip install localstack
	@which awslocal || pip install awscli-local

run:        ## Run the sample queries against the local QLDB API
	go run main.go

start:
	LOCALSTACK_API_KEY=xxxxx DEBUG=1 localstack start -d

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

.PHONY: usage install start run stop logs test-ci

