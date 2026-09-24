# Exercise the built node and CLI executables on temporary ports and fresh data.
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
Push-Location $projectRoot
$processes = @{}
$reservations = @()
$taskDir = Join-Path $projectRoot ('artifacts/smoke-' + [guid]::NewGuid().ToString('N'))
try {
    New-Item -ItemType Directory -Path $taskDir -Force | Out-Null
    go build -o bin/ ./cmd/node ./cmd/client
    if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
    $peers = @()
    foreach ($number in 1..3) {
        $urls = @()
        foreach ($kind in 1..2) {
            $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
            $listener.Start()
            $reservations += $listener
            $urls += 'http://127.0.0.1:' + $listener.LocalEndpoint.Port
        }
        $peers += @{ id = "node-$number"; peerUrl = $urls[0]; clientUrl = $urls[1] }
    }
    $configPath = Join-Path $taskDir 'cluster.json'
    $configJSON = @{ clusterId = 'binary-smoke'; peers = $peers } | ConvertTo-Json -Depth 5
    [IO.File]::WriteAllText($configPath, $configJSON, [Text.UTF8Encoding]::new($false))
    foreach ($listener in $reservations) { $listener.Stop() }
    $reservations = @()
    function Start-SmokeNode($peer) {
        $nodeArgs = @(
            "--id=$($peer.id)",
            ('--cluster="' + $configPath + '"'),
            "--listen=$(([uri]$peer.clientUrl).Authority)",
            "--peer-listen=$(([uri]$peer.peerUrl).Authority)",
            ('--data="' + (Join-Path $taskDir $peer.id) + '"')
        )
        $logName = $peer.id + '-' + [guid]::NewGuid().ToString('N') + '.log'
        $processes[$peer.id] = Start-Process -FilePath (Join-Path $projectRoot 'bin/node.exe') -ArgumentList $nodeArgs -WindowStyle Hidden -PassThru -RedirectStandardError (Join-Path $taskDir $logName)
    }
    function Wait-SmokeLeader {
        $deadline = [datetime]::UtcNow.AddSeconds(15)
        while ([datetime]::UtcNow -lt $deadline) {
            foreach ($peer in $peers) {
                try {
                    $status = Invoke-RestMethod -Uri ($peer.clientUrl + '/v1/status') -TimeoutSec 2
                    if ($status.role -eq 'leader') { return $peer }
                } catch {}
            }
            Start-Sleep -Milliseconds 100
        }
        throw 'No leader elected'
    }
    $servers = ($peers | ForEach-Object { $_.clientUrl }) -join ','
    function Invoke-SmokeClient([string[]]$commands) {
        $output = & (Join-Path $projectRoot 'bin/client.exe') "--servers=$servers" @commands
        if ($LASTEXITCODE -ne 0) { throw "Client failed: $commands" }
        return ($output | ConvertFrom-Json)
    }
    foreach ($peer in $peers) { Start-SmokeNode $peer }
    $leader = Wait-SmokeLeader
    $set = Invoke-SmokeClient @('--client=smoke', '--request=1', 'SET', 'score', '950')
    if (-not $set.applied) { throw 'SET failed' }
    $cas = Invoke-SmokeClient @('--client=smoke', '--request=2', 'CAS', 'score', '950', '1000')
    if (-not $cas.applied) { throw 'CAS failed' }
    Stop-Process -Id $processes[$leader.id].Id -Force
    $processes[$leader.id].WaitForExit()
    $replacement = Wait-SmokeLeader
    if ($replacement.id -eq $leader.id) { throw 'Leader did not change' }
    $retry = Invoke-SmokeClient @('--client=smoke', '--request=2', 'CAS', 'score', '950', '1000')
    if (-not $retry.applied -or $retry.value -ne '1000') { throw 'CAS retry after failover failed' }
    Start-SmokeNode $leader
    $read = Invoke-SmokeClient @('GET', 'score')
    if (-not $read.found -or $read.value -ne '1000') { throw 'Acknowledged value lost' }
    $deleted = Invoke-SmokeClient @('--client=smoke', '--request=3', 'DELETE', 'score')
    if (-not $deleted.applied) { throw 'DELETE failed' }
    Write-Output 'PASS: actual binaries, election, SET/CAS, forced leader kill, retry deduplication, restart, GET/DELETE'
    Write-Output "Logs and isolated test data: $taskDir"
} finally {
    foreach ($listener in $reservations) { $listener.Stop() }
    foreach ($process in $processes.Values) {
        if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force; $process.WaitForExit() }
    }
    Pop-Location
}
