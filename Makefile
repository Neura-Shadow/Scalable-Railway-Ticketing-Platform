GO ?= go
MIGRATE_VERSION ?= v4.19.1
DATABASE_URL ?= postgres://railway:railway-local@localhost:5432/railway?sslmode=disable
IMAGE ?= scalable-railway-ticketing-platform:milestone-1

.PHONY: build test test-race vet fmt-check tidy-check migrate-up migrate-down migrate-status migrate-create compose-up compose-down docker-build

build:
	$(GO) build ./cmd/...

test:
	$(GO) test ./... -count=1 -timeout 300s

test-race:
	$(GO) test -race ./... -count=1 -timeout 300s

vet:
	$(GO) vet ./...

fmt-check:
	@test -z "$$($(GO)fmt -l .)" || { $(GO)fmt -l .; exit 1; }

tidy-check:
	$(GO) mod tidy
	git diff --exit-code -- go.mod go.sum

migrate-up:
	$(GO) run ./cmd/migrate -path migrations -database "$(DATABASE_URL)" up

migrate-down:
	$(GO) run ./cmd/migrate -path migrations -database "$(DATABASE_URL)" down

migrate-status:
	$(GO) run ./cmd/migrate -path migrations -database "$(DATABASE_URL)" version

migrate-create:
	@test -n "$(name)" || { echo "usage: make migrate-create name=description"; exit 2; }
	$(GO) run github.com/golang-migrate/migrate/v4/cmd/migrate@$(MIGRATE_VERSION) create -ext sql -dir migrations -seq "$(name)"

compose-up:
	docker compose up --build

compose-down:
	docker compose down

docker-build:
	docker build -t "$(IMAGE)" .
