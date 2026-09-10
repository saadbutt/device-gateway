.PHONY: all build test race lint bench run sim docker clean

all: test build

build:
	go build -trimpath -o bin/gateway ./cmd/gateway
	go build -trimpath -o bin/simulator ./cmd/simulator

test:
	go test ./... -count=1

race:
	go test ./... -race -count=1

# vet is the lint floor. gofmt -l printing anything is a failure.
lint:
	@test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "gofmt found unformatted files"; exit 1)
	go vet ./...

bench:
	go test ./... -bench=. -benchmem -run=^$$

run: build
	./bin/gateway -addr :5027 -metrics-addr :9100

sim: build
	./bin/simulator -addr localhost:5027 -devices 200 -batch 20 -drop-rate 0.2 -storm 30s

docker:
	docker build -t device-gateway .

clean:
	rm -rf bin
