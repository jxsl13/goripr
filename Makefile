

up:
	docker compose up -d

down:
	docker compose down

test:
	go test -timeout 1800s -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...