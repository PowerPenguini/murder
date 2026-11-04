#!/usr/bin/env bash
# Reads every tenant through every edge until STOPFILE exists, one loop per
# edge, so an edge that is down (each request waiting for its timeout) does
# not starve the others, and logs each answer with its time, so failures
# can be counted for any window afterwards (phases of a test):
#
#   steady.sh DOMAIN "node=ip ..." "tenant ..." LOG STOPFILE
#
# Each line: epoch-seconds node tenant result http-code seconds, where
# result is ok or FAILED (an answer other than "value of <tenant>").
#
#   awk -v a=START -v b=END '$1>=a && $1<b {n[$4]++} END {print n["ok"]+0, n["FAILED"]+0}' LOG
set -uo pipefail

domain=$1 log=$4 stop=$5
read -ra edges <<< "$2"
read -ra tenants <<< "$3"
one_edge() {
  local node=${1%%=*} ip=${1#*=} tenant out took rest code result
  while [[ ! -e $stop ]]; do
    for tenant in "${tenants[@]}"; do
      out=$(curl -sk -m 20 --resolve "$tenant.$domain:443:$ip" -w ' %{http_code} %{time_total}' "https://$tenant.$domain/value") || out="curl-error-$? 000 0"
      took=${out##* }; rest=${out% *}; code=${rest##* }
      if [[ $out == "value of $tenant 200 "* ]]; then result=ok; else result=FAILED; fi
      # One short line per write: appends of it do not interleave.
      printf '%s %s %s %s %s %s\n' "$(date +%s.%3N)" "$node" "$tenant" "$result" "$code" "$took" >> "$log"
    done
  done
}
for edge in "${edges[@]}"; do one_edge "$edge" & done
wait
