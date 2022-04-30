postgres:
	docker run --name postgres14 -p 5432:5432 -e POSTGRES_PASSWORD=oracle -d postgres

sqlc:
	sqlc generate

build:
    go build

test:
	go test -v -cover ./...

.PHONY: postgres sqlc build test
