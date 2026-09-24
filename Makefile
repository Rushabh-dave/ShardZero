.PHONY: run cluster lab simulate benchmark release dashboard test race vet build check
run:
	go run ./cmd/node
cluster:
	go run ./cmd/cluster
lab:
	go run ./cmd/lab
simulate:
	go run ./cmd/simulator --seed=728391 --operations=80
benchmark:
	go test -run '^$$' -bench BenchmarkReplicatedWrite -benchmem ./internal/raft
release:
	powershell -ExecutionPolicy Bypass -File scripts/phase9-release.ps1
dashboard:
	npm --prefix dashboard ci
	npm --prefix dashboard run build
test:
	go test ./...
race:
	go test -race ./...
vet:
	go vet ./...
build:
	go build -o bin/ ./cmd/node ./cmd/client ./cmd/cluster ./cmd/simulator ./cmd/lab
check: vet race build
