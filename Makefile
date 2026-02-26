BIN := bin/dbsp
CMD := ./cmd/dbsp

.PHONY: build test clean

build:
	go build -o $(BIN) $(CMD)

test:
	go test ./...

clean:
	rm -f $(BIN)
