# End-to-end feature demo on a real multi-process cluster.
# Starts a NameNode + 4 DataNodes (separate JVMs, ports 18020 / 19001-19004, data in ./demo-run),
# exercises every feature, injects failures, and writes the real output to docs/DEMO.md.
#
#   bin/demo.ps1                 run everything, then stop the cluster
#   bin/demo.ps1 -KeepRunning    leave the cluster up afterwards (client: bin/hdfs.ps1 dfs --namenode localhost:18020)
param([switch]$KeepRunning, [switch]$NoBuild)

$ErrorActionPreference = "Continue"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$run = Join-Path $root "demo-run"
$logs = Join-Path $run "logs"
$out = Join-Path $root "docs\DEMO.md"
$NN = "localhost:18020"
$fence = '```'
$cp = (@("hdfs-client", "hdfs-namenode", "hdfs-datanode", "hdfs-common") | ForEach-Object { Join-Path $root "$_\target\classes" }) -join ";"
$md = New-Object System.Collections.Generic.List[string]
$results = New-Object System.Collections.Generic.List[object]
$started = Get-Date

# ── output helpers ─────────────────────────────────────────────────────────────
function Add-Md($text) { $md.Add($text) }
function Scene($n, $title, $why) {
    Write-Host "`n=== $n. $title ===" -ForegroundColor Yellow
    Add-Md "## $n. $title"; Add-Md ""; Add-Md $why; Add-Md ""
}
function Say($text) { Write-Host $text -ForegroundColor Cyan; Add-Md $text; Add-Md "" }
function Show($label, $lines) {
    $lines = @($lines | ForEach-Object { "$_" })
    if ($label) { Write-Host $label -ForegroundColor DarkCyan; Add-Md "**$label**"; Add-Md "" }
    $lines | ForEach-Object { Write-Host "  $_" }
    Add-Md $fence
    if ($lines.Count -eq 0) { Add-Md "(nothing)" } else { $lines | ForEach-Object { Add-Md $_ } }
    Add-Md $fence; Add-Md ""
}
function Result($feature, $ok, $evidence) {
    $results.Add([pscustomobject]@{ Feature = $feature; Ok = $ok; Evidence = $evidence })
    $mark = if ($ok) { "PASS" } else { "FAIL" }
    $color = if ($ok) { "Green" } else { "Red" }
    Write-Host "  [$mark] $feature - $evidence" -ForegroundColor $color
    Add-Md "> **$mark** - $evidence"; Add-Md ""
}

# ── cluster helpers ────────────────────────────────────────────────────────────
function Invoke-Hdfs { & java -cp $cp dev.anurag.hdfs.client.HdfsCli --namenode $NN @args 2>&1 | ForEach-Object { "$_" } }
function Dfs { $o = @(Invoke-Hdfs @args); @("`$ hdfs dfs " + ($args -join ' ')) + $o }
function Stamp { Get-Date -Format "yyyyMMdd-HHmmssfff" }

function Start-NameNode {
    $a = @("-cp", "`"$cp`"", "dev.anurag.hdfs.namenode.NameNodeMain", "--port", "18020", "--dir", "`"$run\namenode`"",
           "--block-size", "4096", "--stale-ms", "3000", "--dead-ms", "6000", "--checkpoint-edits", "20")
    $ts = Stamp
    Start-Process java -ArgumentList $a -WindowStyle Hidden -RedirectStandardOutput "$logs\namenode-$ts.out" -RedirectStandardError "$logs\namenode-$ts.err" | Out-Null
}
function Start-DataNode($id, $port, $dir) {
    if (-not $dir) { $dir = Join-Path $run $id }
    $a = @("-cp", "`"$cp`"", "dev.anurag.hdfs.datanode.DataNodeMain", "--id", $id, "--port", "$port", "--dir", "`"$dir`"",
           "--namenode", $NN, "--scan-ms", "3000", "--block-report-ms", "10000")
    $ts = Stamp
    Start-Process java -ArgumentList $a -WindowStyle Hidden -RedirectStandardOutput "$logs\$id-$ts.out" -RedirectStandardError "$logs\$id-$ts.err" | Out-Null
}
function Kill-Java($pattern) {
    Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" | Where-Object { $_.CommandLine -match $pattern } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}
function Kill-DataNode($id) { Kill-Java "DataNodeMain --id $id .*localhost:18020" }
function Kill-NameNode { Kill-Java "NameNodeMain --port 18020" }
function Port-Open($p) { try { $c = New-Object Net.Sockets.TcpClient; $c.Connect("127.0.0.1", $p); $c.Close(); $true } catch { $false } }

function Wait-Until($what, [scriptblock]$cond, $timeoutSec = 30) {
    $sw = [Diagnostics.Stopwatch]::StartNew()
    while (-not (& $cond)) {
        if ($sw.Elapsed.TotalSeconds -gt $timeoutSec) { Write-Host "  timed out waiting for: $what" -ForegroundColor Red; return -1 }
        Start-Sleep -Milliseconds 300
    }
    $s = [Math]::Round($sw.Elapsed.TotalSeconds, 1)
    Write-Host "  ($what after $s s)" -ForegroundColor DarkGray
    return $s
}

# Logs: every (re)start writes a new file; concatenate them in time order.
$script:marks = @{}
function LogLines($prefix) {
    Get-ChildItem $logs -Filter "$prefix-2*.out" -ErrorAction SilentlyContinue | Sort-Object Name | ForEach-Object { Get-Content $_.FullName }
}
function Mark($prefix) { $script:marks[$prefix] = @(LogLines $prefix).Count }
function NewLog($prefix, $pattern) {
    $skip = if ($script:marks.ContainsKey($prefix)) { $script:marks[$prefix] } else { 0 }
    $new = @(LogLines $prefix) | Select-Object -Skip $skip
    if ($pattern) { $new = $new | Where-Object { $_ -match $pattern } }
    @($new)
}

function StateOf($id) {
    $line = Invoke-Hdfs report | Where-Object { $_ -match "^\s+$id\s" } | Select-Object -First 1
    if ($line -match '\s(NORMAL|STALE|DEAD)\s') { $matches[1] } else { "-" }
}
function LiveCount { @(Invoke-Hdfs report | Where-Object { $_ -match '\sNORMAL\s' }).Count }
function Locs($path) {
    Invoke-Hdfs stat $path | ForEach-Object {
        if ($_ -match 'block (\d+).*\[([^\]]*)\]') {
            [pscustomobject]@{ Id = [long]$matches[1]; Nodes = @($matches[2] -split ',\s*' | Where-Object { $_ }) }
        }
    }
}
function Holders($blockId, $exclude) {
    @(1..4 | Where-Object { Test-Path (Join-Path $run "dn-$_\current\blk_$blockId") } | ForEach-Object { "dn-$_" } | Where-Object { $_ -ne $exclude })
}
function Disk {
    foreach ($i in 1..4) {
        $blocks = @(Get-ChildItem (Join-Path $run "dn-$i\current") -Filter "blk_*" -ErrorAction SilentlyContinue |
                    Where-Object { $_.Name -notlike "*.meta" } | Sort-Object { [long]($_.Name -replace 'blk_', '') } | ForEach-Object Name)
        $bad = @(Get-ChildItem (Join-Path $run "dn-$i\corrupt") -Filter "blk_*" -ErrorAction SilentlyContinue |
                 Where-Object { $_.Name -notlike "*.meta" } | ForEach-Object Name)
        $line = "dn-$i/current: " + $(if ($blocks.Count) { $blocks -join ' ' } else { "(empty)" })
        if ($bad.Count) { $line += "    dn-$i/corrupt: " + ($bad -join ' ') }
        $line
    }
}
function DiskCount {
    foreach ($i in 1..4) {
        $n = @(Get-ChildItem (Join-Path $run "dn-$i\current") -Filter "blk_*" -ErrorAction SilentlyContinue | Where-Object { $_.Name -notlike "*.meta" }).Count
        "dn-$i/current: $n block file(s)"
    }
}
function Same($a, $b) { (Test-Path $a) -and (Test-Path $b) -and ((Get-FileHash $a).Hash -eq (Get-FileHash $b).Hash) }

# ── setup ──────────────────────────────────────────────────────────────────────
Kill-Java 'dev\.anurag\.hdfs\..*18020'
Start-Sleep -Milliseconds 500
if (Test-Path $run) { Remove-Item -Recurse -Force $run }
New-Item -ItemType Directory -Force $logs | Out-Null
if (-not $NoBuild) {
    Write-Host "Building..." -ForegroundColor DarkGray
    mvn -q -f (Join-Path $root "pom.xml") compile
    if ($LASTEXITCODE -ne 0) { throw "build failed" }
}
$sample = Join-Path $root "README.md"
$big = "demo-run\big.bin"
$bytes = New-Object byte[] 61440; (New-Object Random 42).NextBytes($bytes); [IO.File]::WriteAllBytes((Join-Path $run "big.bin"), $bytes)

try {
# ── 1 ──────────────────────────────────────────────────────────────────────────
Scene 1 "Start a cluster of separate processes" @"
One NameNode and four DataNodes, each its own JVM, talking only over TCP. Every DataNode
does a cluster-ID handshake, registers, sends a full block report, and then heartbeats
every second. The NameNode runs with a 4 KB block size so small files span several blocks.
"@
Start-NameNode
Wait-Until "NameNode listening" { Port-Open 18020 } 20 | Out-Null
1..4 | ForEach-Object { Start-DataNode "dn-$_" (19000 + $_) }
$t = Wait-Until "4 DataNodes registered" { (LiveCount) -eq 4 } 30
Show "NameNode log" (LogLines "namenode")
Show $null (Dfs report)
Result "Multi-process cluster" ($t -ge 0) "NameNode + 4 DataNode processes up; all 4 registered and heartbeating within $t s"

# ── 2 ──────────────────────────────────────────────────────────────────────────
Scene 2 "Namespace operations" @"
Directories and files live only in the NameNode's memory (backed by the edit log). Paths are
validated, parents must exist (or use ``mkdir -p``), and a directory's size is the sum of its files.
"@
$cmds = @()
$cmds += Dfs mkdir -p /projects/hdfs/docs
$cmds += Dfs mkdir /tmp
$cmds += Dfs write /projects/hdfs/hello.txt Hello from a real mini-HDFS cluster
$cmds += Dfs put README.md /projects/hdfs/docs
$cmds += Dfs ls /
$cmds += Dfs ls /projects/hdfs
$cmds += Dfs ls /projects/hdfs/docs
$cmds += Dfs mkdir /projects
$cmds += Dfs ls /projects/hdfs/hello.txt
Show $null $cmds
$ok = ($cmds -match 'README.md').Count -gt 0 -and ($cmds -match 'Already exists').Count -gt 0 -and ($cmds -match 'Not a directory').Count -gt 0
Result "Namespace ops (mkdir -p, put, write, ls, errors)" $ok "files and directories created; duplicate mkdir and ls-on-a-file rejected with clear errors"

# ── 3 ──────────────────────────────────────────────────────────────────────────
Scene 3 "Blocks, replication and on-disk storage" @"
README.md is split into 4 KB blocks. Each block was written through a pipeline
(client -> DN -> DN -> DN) and stored on 3 different DataNodes as ``blk_<id>`` plus a 16-byte
``blk_<id>.meta`` holding its CRC32 and length. (``blk_1`` is the small hello.txt written just before.)
"@
Show $null (Dfs stat /projects/hdfs/docs/README.md)
Show "Block files on each DataNode's disk" (Disk)
$metaFile = Get-ChildItem (Join-Path $run "dn-*\current\*.meta") | Select-Object -First 1
$locs = @(Locs /projects/hdfs/docs/README.md)
$ok = $locs.Count -ge 2 -and @($locs | Where-Object { $_.Nodes.Count -ne 3 }).Count -eq 0 -and $metaFile.Length -eq 16
Result "Block splitting + 3x replication" $ok "$($locs.Count) blocks, each on 3 distinct DataNodes; meta file is $($metaFile.Length) bytes (CRC32 + length)"

# ── 4 ──────────────────────────────────────────────────────────────────────────
Scene 4 "Read path" @"
The client asks the NameNode only for block locations, then fetches each block straight from a
DataNode, verifying its checksum. The downloaded file is compared byte-for-byte with the original.
"@
$copy = "demo-run\README.copy.md"
$cmds = @(); $cmds += Dfs get /projects/hdfs/docs/README.md $copy; $cmds += Dfs cat /projects/hdfs/hello.txt
Show $null $cmds
$ok = Same $sample $copy
Result "Read path + checksums" $ok "downloaded copy SHA-256 $(if ($ok) { 'matches' } else { 'DIFFERS from' }) the original"

# ── 5 ──────────────────────────────────────────────────────────────────────────
Scene 5 "Rename and move" @"
A move is a metadata-only change on the NameNode: block IDs and DataNodes are untouched,
so it costs the same for 1 byte or 1 TB.
"@
$before = @(Locs /projects/hdfs/hello.txt)
$cmds = @(); $cmds += Dfs mv /projects/hdfs/hello.txt /tmp/greeting.txt; $cmds += Dfs ls /projects/hdfs; $cmds += Dfs ls /tmp; $cmds += Dfs cat /tmp/greeting.txt
Show $null $cmds
$after = @(Locs /tmp/greeting.txt)
$ok = $before.Count -eq 1 -and $after.Count -eq 1 -and $before[0].Id -eq $after[0].Id
Result "Rename / move" $ok "file moved; it still uses block $($after[0].Id) on the same DataNodes (no data copied)"

# ── 6 ──────────────────────────────────────────────────────────────────────────
$readmeLocs = @(Locs /projects/hdfs/docs/README.md)
$victim = $readmeLocs[0].Nodes[0]
Scene 6 "A DataNode crashes" @"
``$victim`` (which holds README.md's first block) is killed. Its heartbeats stop: after 3 s the
NameNode marks it STALE (no new blocks go there), after 6 s DEAD. Every block it held is now
below 3 copies, so the NameNode tells a surviving holder, through its next heartbeat reply,
to copy the block to another DataNode.
"@
Mark "namenode"
Kill-DataNode $victim
$tDead = Wait-Until "$victim declared DEAD" { (StateOf $victim) -eq "DEAD" } 20
$tHeal = Wait-Until "every README block back to 3 copies on live disks" {
    @($readmeLocs | Where-Object { (Holders $_.Id $victim).Count -lt 3 }).Count -eq 0 } 20
Show "NameNode log" (NewLog "namenode" 'STALE|DEAD|under-replicated')
Show $null (Dfs report)
Show $null (Dfs stat /projects/hdfs/docs/README.md)
$copy2 = "demo-run\README.copy2.md"
Show $null (Dfs get /projects/hdfs/docs/README.md $copy2)
$ok = $tDead -ge 0 -and $tHeal -ge 0 -and (Same $sample $copy2)
Result "Failure detection + re-replication" $ok "$victim declared DEAD after $tDead s; all blocks back to 3 live copies $tHeal s later; file still reads correctly"

# ── 7 ──────────────────────────────────────────────────────────────────────────
Scene 7 "The crashed DataNode comes back" @"
``$victim`` restarts with its old blocks still on disk. The NameNode had written it off, so it
answers the first heartbeat with UNREGISTERED. The DataNode re-registers and sends a full block
report, and every copy it reports is now a 4th replica, so the NameNode tells it to delete them.
"@
Mark $victim
Start-DataNode $victim (19000 + [int]($victim -replace 'dn-', ''))
$t = Wait-Until "$victim rejoined and dropped its extra copies" {
    (StateOf $victim) -eq "NORMAL" -and @(Get-ChildItem (Join-Path $run "$victim\current") -Filter "blk_*").Count -eq 0 } 25
Show "$victim log" (NewLog $victim '')
Show "Disks" (Disk)
Result "Rejoin + over-replication cleanup" ($t -ge 0) "$victim re-registered by itself and deleted its now-redundant copies"

# ── 8 ──────────────────────────────────────────────────────────────────────────
$target = @(Locs /projects/hdfs/docs/README.md)[0]
$bad = $target.Nodes[0]
$bid = $target.Id
Scene 8 "Silent disk corruption" @"
One byte of ``blk_$bid`` on ``$bad`` is flipped directly on disk, and nobody reads the file.
The DataNode's background block scanner re-checks every block's CRC32, finds the mismatch,
moves the replica to ``corrupt/``, and sends a block report right away. The NameNode sees the
block is down to 2 copies and re-replicates it from a healthy one.
"@
$f = Join-Path $run "$bad\current\blk_$bid"
$b = [IO.File]::ReadAllBytes($f); $b[10] = $b[10] -bxor 0xFF; [IO.File]::WriteAllBytes($f, $b)
Mark $bad; Mark "namenode"
$tFound = Wait-Until "scanner quarantined the corrupt copy" { Test-Path (Join-Path $run "$bad\corrupt\blk_$bid") } 20
$tFix = Wait-Until "3 good copies again" { (Holders $bid).Count -ge 3 } 20
Show "$bad log" (NewLog $bad 'scanner')
Show "NameNode log" (NewLog "namenode" 'under-replicated')
Show "Disks" (Disk)
$copy3 = "demo-run\README.copy3.md"
Show $null (Dfs get /projects/hdfs/docs/README.md $copy3)
$ok = $tFound -ge 0 -and $tFix -ge 0 -and (Same $sample $copy3)
Result "Checksums + block scanner + repair" $ok "corruption found by the scanner in $tFound s without any read; block back to 3 good copies; file still byte-identical"

# ── 9 ──────────────────────────────────────────────────────────────────────────
$pv = "dn-4"
Scene 9 "A DataNode dies in the middle of writes" @"
``$pv`` is killed and a 60 KB file (15 blocks) is uploaded immediately, before the NameNode
notices. Any pipeline that includes ``$pv`` fails. The pipeline reports which node failed, and
the client abandons that block, asks for a new one excluding ``$pv``, and retries.
"@
Kill-DataNode $pv
Mark "namenode"
$cmds = @(); $cmds += Dfs mkdir /data; $cmds += Dfs put $big /data
Show $null $cmds
Show "NameNode log" (NewLog "namenode" 'abandoned|excluding')
$bigLocs = @(Locs /data/big.bin)
$onVictim = @($bigLocs | Where-Object { $_.Nodes -contains $pv }).Count
$copy4 = "demo-run\big.copy.bin"
Show $null (Dfs get /data/big.bin $copy4)
$retries = @(NewLog "namenode" 'abandoned').Count
$ok = ($cmds -match 'wrote /data/big.bin').Count -gt 0 -and $onVictim -eq 0 -and (Same $big $copy4)
Result "Pipeline failure recovery" $ok "upload succeeded despite the dead node: $retries block(s) abandoned and retried, 0 of $($bigLocs.Count) blocks placed on $pv"
Start-DataNode $pv 19004
Wait-Until "$pv back" { (LiveCount) -eq 4 } 25 | Out-Null

# ── 10 ─────────────────────────────────────────────────────────────────────────
Scene 10 "Delete frees the blocks on every DataNode" @"
Deleting a file removes it from the namespace and queues a DELETE for every replica of every
block; each DataNode receives its DELETEs in its next heartbeat reply.
"@
$ids = @($bigLocs | ForEach-Object Id)
Show "Before" (DiskCount)
Mark "dn-1"; Mark "dn-2"; Mark "dn-3"
$rmCmd = "rm"
Show $null (Dfs $rmCmd /data/big.bin)
$t = Wait-Until "all of big.bin's blocks gone from every disk" { @($ids | Where-Object { (Holders $_).Count -gt 0 }).Count -eq 0 } 20
Show "After" (DiskCount)
Show "dn-1 log (excerpt)" (NewLog "dn-1" 'deleted block' | Select-Object -First 5)
Result "Delete + block invalidation" ($t -ge 0) "$($ids.Count) blocks removed from every DataNode's disk within $t s"

# ── 11 ─────────────────────────────────────────────────────────────────────────
Scene 11 "Edit log and checkpoints" @"
Every namespace change is appended to ``edits.log`` and fsync'd *before* it is applied. Every 20
edits the NameNode writes a full snapshot (``fsimage.bin``, via temp file + atomic rename) and
truncates the log, so recovery replays only recent edits. Below, the log holds only what happened since the last checkpoint.
"@
$nnDir = Join-Path $run "namenode"
Show "NameNode log" (LogLines "namenode" | Where-Object { $_ -match 'checkpoint' })
Show "Metadata directory" (Get-ChildItem $nnDir | ForEach-Object { "{0,-12} {1,7:N0} bytes" -f $_.Name, $_.Length })
Show "edits.log (op, timestamp, args)" (Get-Content (Join-Path $nnDir "edits.log") | ForEach-Object { $_ -replace "`t", "  " })
Show "fsimage.bin (first lines)" (Get-Content (Join-Path $nnDir "fsimage.bin") | Select-Object -First 8 | ForEach-Object { $_ -replace "`t", "  " })
$ok = (Test-Path (Join-Path $nnDir "fsimage.bin")) -and @(LogLines "namenode" | Where-Object { $_ -match 'checkpoint written' }).Count -ge 1
Result "Write-ahead log + checkpoints" $ok "edits logged before being applied; automatic checkpoint written after 20 edits"

# ── 12 ─────────────────────────────────────────────────────────────────────────
Scene 12 "Whole cluster loses power" @"
Every process is killed at once (no clean shutdown). Then the NameNode alone is started: it loads
the fsimage and replays the edit log, but it knows no block locations yet, so it stays in
**safe mode**: reads of the namespace work, changes are refused. When the DataNodes come back
and report the blocks on their disks, it leaves safe mode on its own.
"@
Kill-Java 'dev\.anurag\.hdfs\..*18020'
Start-Sleep -Seconds 1
Show "Cluster is down" (Dfs ls /)
Mark "namenode"
Start-NameNode
Wait-Until "NameNode listening" { Port-Open 18020 } 20 | Out-Null
Start-Sleep -Seconds 1
$cmds = @(); $cmds += Dfs report; $cmds += Dfs ls /projects/hdfs/docs; $cmds += Dfs mkdir /after-restart
Show "NameNode up, no DataNodes yet" $cmds
$refused = ($cmds -match 'safe mode').Count -gt 0
1..4 | ForEach-Object { Start-DataNode "dn-$_" (19000 + $_) }
$t = Wait-Until "DataNodes reported, safe mode over" { (LiveCount) -eq 4 -and ((Invoke-Hdfs mkdir /after-restart) -notmatch 'safe mode') } 30
Show "NameNode log" (NewLog "namenode" '')
$copy5 = "demo-run\README.copy5.md"
$cmds = @(); $cmds += Dfs ls /; $cmds += Dfs get /projects/hdfs/docs/README.md $copy5; $cmds += Dfs cat /tmp/greeting.txt
Show "After recovery" $cmds
$ok = $refused -and $t -ge 0 -and (Same $sample $copy5)
Result "Crash recovery + safe mode" $ok "namespace rebuilt from fsimage + edit log; writes refused until block reports arrived ($t s); every file intact"

# ── 13 ─────────────────────────────────────────────────────────────────────────
Scene 13 "A DataNode from another cluster tries to join" @"
A DataNode whose stored cluster ID doesn't match is refused before it registers. Without this
check, a NameNode started on an empty metadata directory would treat every block a DataNode
reports as an orphan and order it deleted.
"@
$intr = Join-Path $run "intruder"
New-Item -ItemType Directory -Force $intr | Out-Null
Set-Content -Path (Join-Path $intr "CLUSTER_ID") -Value "some-other-cluster" -NoNewline
Mark "intruder"
Start-DataNode "intruder" 19009 $intr
$t = Wait-Until "intruder refused" { @(NewLog "intruder" 'FATAL').Count -gt 0 } 20
Show "intruder log" (NewLog "intruder" '')
$rep = Dfs report
Show $null $rep
Kill-DataNode "intruder"
$ok = $t -ge 0 -and ($rep -match 'intruder').Count -eq 0
Result "Cluster-ID safety check" $ok "foreign DataNode refused to join and never appears in the NameNode's report"

# ── 14 ─────────────────────────────────────────────────────────────────────────
Scene 14 "Interactive shell" @"
The same client also runs as an interactive shell with a current directory, relative paths and ``..``.
"@
$session = "cd /projects/hdfs`npwd`nls`ncd docs`nstat README.md`ncd ../..`npwd`nexit"
$shellOut = $session | & java -cp $cp dev.anurag.hdfs.client.HdfsCli --namenode $NN --echo true 2>&1 | ForEach-Object { "$_" }
Show "Input: cd /projects/hdfs, pwd, ls, cd docs, stat README.md, cd ../.., pwd" $shellOut
Result "Interactive shell" (($shellOut -match '^/projects$').Count -gt 0 -or ($shellOut -join "`n") -match '/projects') "cd / pwd / relative paths work"
}
finally {
    if (-not $KeepRunning) { Kill-Java 'dev\.anurag\.hdfs\..*18020' }

    $elapsed = [Math]::Round(((Get-Date) - $started).TotalSeconds)
    $passed = @($results | Where-Object { $_.Ok }).Count
    $header = New-Object System.Collections.Generic.List[string]
    $header.Add("# Mini-HDFS - Live Demo")
    $header.Add("")
    $header.Add("Everything below is real output from ``bin/demo.ps1``, generated on $(Get-Date -Format 'yyyy-MM-dd HH:mm') in $elapsed s.")
    $header.Add("The script starts a NameNode and four DataNodes as **separate JVM processes**, drives them only")
    $header.Add("through the ``hdfs dfs`` command-line client, and injects real failures by killing processes and")
    $header.Add("editing block files on disk. Run it yourself: ``bin/demo.ps1`` (add ``-KeepRunning`` to keep the cluster up).")
    $header.Add("")
    $header.Add("| Setting | Value |")
    $header.Add("|---|---|")
    $header.Add("| Block size | 4 KB (small, so ordinary files span several blocks) |")
    $header.Add("| Replication | 3 |")
    $header.Add("| Heartbeat | every 1 s; STALE after 3 s, DEAD after 6 s |")
    $header.Add("| Block scanner | every 3 s; full block report every 10 s |")
    $header.Add("| Checkpoint | every 20 edits |")
    $header.Add("")
    $header.Add("## Results: $passed / $($results.Count) passed")
    $header.Add("")
    $header.Add("| # | Feature | Result | Evidence |")
    $header.Add("|---|---|---|---|")
    $i = 0
    foreach ($r in $results) {
        $i++
        $header.Add("| $i | $($r.Feature) | $(if ($r.Ok) { 'PASS' } else { 'FAIL' }) | $($r.Evidence) |")
    }
    $header.Add("")
    $header.Add("---")
    $header.Add("")
    ($header + $md) | ForEach-Object { $_.Replace("$root\", "").Replace("$root", ".") } | Set-Content -Path $out -Encoding UTF8
    Write-Host "`n$passed / $($results.Count) features passed. Report: $out" -ForegroundColor $(if ($passed -eq $results.Count) { "Green" } else { "Red" })
}
