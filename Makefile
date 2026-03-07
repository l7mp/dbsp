BIN := bin/dbsp
CMD := ./cmd/dbsp

.PHONY: build test clean

build:
	go build -o $(BIN) $(CMD)

test:
	go test ./... -v -count 1 

clean:
	rm -f $(BIN)
