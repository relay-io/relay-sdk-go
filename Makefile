deps:
	go mod download
	go mod tidy

linters-install:
	@golangci-lint --version >/dev/null 2>&1 || { \
		echo "installing linting tools..."; \
		curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s v1.41.1; \
	}

lint: linters-install deps
	golangci-lint run

test: lint
	RELAY_URL="http://127.0.0.1:8080" \
	go test -cover -race ./...

test.ci: deps
	go test -cover -race ./...

services.up:
	@docker-compose up -d
	@dockerize -wait tcp://:5432 -wait tcp://:8080

services.down:
	@docker-compose stop --timeout 0

test.all: services.up test services.down

bench:
	go test -bench=. -benchmem ./...

.PHONY: deps test test.ci test.all lint linters-install services.up services.down