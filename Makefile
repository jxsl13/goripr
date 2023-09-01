

environment:
	docker-compose up -d

test:
	go test -v -timeout 1800s -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...