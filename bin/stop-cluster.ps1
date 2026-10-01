# Kills every NameNode / DataNode JVM started from this repo.
$procs = Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" |
         Where-Object { $_.CommandLine -match "dev\.anurag\.hdfs\.(namenode\.NameNodeMain|datanode\.DataNodeMain)" }
foreach ($p in $procs) { Stop-Process -Id $p.ProcessId -Force }
Write-Host "Stopped $(@($procs).Count) process(es)."
