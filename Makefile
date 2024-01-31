

up:
	docker compose -f docker-compose.old.yaml up -d

down:
	docker compose -f docker-compose.old.yaml down

old-up:
	docker compose -f docker-compose.old.yaml up -d

old-down:
	docker compose -f docker-compose.old.yaml down

test:
	go test -timeout 1800s -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...