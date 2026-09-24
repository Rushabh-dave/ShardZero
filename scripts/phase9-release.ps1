param(
  [int]$Seeds = 10000,
  [int]$Operations = 80,
  [string]$Out = "artifacts/phase9-release"
)

$ErrorActionPreference = "Stop"
go test ./...
go vet ./...
go test -run '^$' -bench BenchmarkReplicatedWrite -benchmem ./internal/raft
go run ./cmd/simulator --seed=728391 --nodes=3 --operations=$Operations --runs=$Seeds --out=$Out
