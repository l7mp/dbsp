BIN := bin/dbsp
CMD := .

.PHONY: build test clean

build:
	go build -o $(BIN) $(CMD)

test:
	go test ./... -v -count 1 

clean:
	rm -f $(BIN)
