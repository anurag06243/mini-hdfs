# Starts a NameNode and N DataNodes, each in its own window, with data under ./data.
#   bin/start-cluster.ps1 [-DataNodes 3]
param([int]$DataNodes = 3)
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
mvn -q -f (Join-Path $root "pom.xml") compile
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$shell = (Get-Process -Id $PID).Path
$hdfs = Join-Path $PSScriptRoot "hdfs.ps1"
$data = Join-Path $root "data"

function Start-Node([string]$title, [string]$arguments) {
    $cmd = "`$host.UI.RawUI.WindowTitle = '$title'; & '$hdfs' $arguments"
    Start-Process $shell -ArgumentList "-NoExit", "-Command", $cmd | Out-Null
}

Start-Node "NameNode" "namenode --port 8020 --dir '$data/namenode'"
Start-Sleep -Seconds 2
for ($i = 1; $i -le $DataNodes; $i++) {
    Start-Node "dn-$i" "datanode --id dn-$i --port $(9000 + $i) --dir '$data/dn-$i' --namenode localhost:8020"
}
Write-Host "Started NameNode (localhost:8020) and $DataNodes DataNodes (ports 9001..$(9000 + $DataNodes))."
Write-Host "Client:  bin/hdfs.ps1 dfs          Stop: bin/stop-cluster.ps1 (or close the windows)"
