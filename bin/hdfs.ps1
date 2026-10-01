# Usage:
#   bin/hdfs.ps1 namenode [--port 8020] [--dir data/namenode] [--block-size 1048576]
#   bin/hdfs.ps1 datanode --id dn-1 --port 9001 [--dir data/dn-1] [--namenode localhost:8020]
#   bin/hdfs.ps1 dfs [--namenode localhost:8020] [command args...]     (no command = interactive shell)
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
$modules = "hdfs-client", "hdfs-namenode", "hdfs-datanode", "hdfs-common"
if (-not (Test-Path (Join-Path $root "hdfs-client/target/classes/dev"))) {
    mvn -q -f (Join-Path $root "pom.xml") compile
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
$cp = ($modules | ForEach-Object { Join-Path $root "$_/target/classes" }) -join [IO.Path]::PathSeparator

$command = if ($args.Count -gt 0) { $args[0] } else { "" }
$rest = @($args | Select-Object -Skip 1)
switch ($command) {
    "namenode" { java -cp $cp dev.anurag.hdfs.namenode.NameNodeMain @rest }
    "datanode" { java -cp $cp dev.anurag.hdfs.datanode.DataNodeMain @rest }
    "dfs"      { java -cp $cp dev.anurag.hdfs.client.HdfsCli @rest }
    default    { Write-Host "usage: hdfs.ps1 (namenode | datanode | dfs) [options]"; exit 2 }
}
exit $LASTEXITCODE
