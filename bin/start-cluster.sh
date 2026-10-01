#!/usr/bin/env bash
# Starts a NameNode and N DataNodes in the background; logs and pids go to data/logs.
#   bin/start-cluster.sh [num-datanodes]
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
n="${1:-3}"
mvn -q -f "$root/pom.xml" compile
mkdir -p "$root/data/logs"

"$root/bin/hdfs" namenode --port 8020 --dir "$root/data/namenode" > "$root/data/logs/namenode.log" 2>&1 &
echo $! > "$root/data/logs/namenode.pid"
sleep 2
for i in $(seq 1 "$n"); do
  "$root/bin/hdfs" datanode --id "dn-$i" --port $((9000 + i)) --dir "$root/data/dn-$i" \
      --namenode localhost:8020 > "$root/data/logs/dn-$i.log" 2>&1 &
  echo $! > "$root/data/logs/dn-$i.pid"
done
echo "Started NameNode (localhost:8020) and $n DataNodes. Logs: data/logs/"
echo "Client: bin/hdfs dfs     Kill one DataNode: kill \$(cat data/logs/dn-1.pid)     Stop all: bin/stop-cluster.sh"
