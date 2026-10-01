$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
mvn -q clean compile
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$cp = @("hdfs-client", "hdfs-namenode", "hdfs-datanode", "hdfs-common") |
      ForEach-Object { Join-Path $_ "target\classes" }
java -cp ($cp -join ";") dev.anurag.hdfs.client.Demo
