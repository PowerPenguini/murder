#!/usr/bin/env bash
# Reads every tenant through every edge, one request after the other, for
# SECONDS; an answer other than the tenant's value is a failure. nodes.sh
# runs it on the client server, inside Hetzner's network, when there is one.
#
#   load.sh SECONDS DOMAIN "node=ip ..." "tenant ..."
set -uo pipefail

seconds=$1 domain=$2
read -ra edges <<< "$3"
read -ra tenants <<< "$4"
ok=0 failed=0 slow=0 times=()
end=$((SECONDS + seconds))
while ((SECONDS < end)); do
  for edge in "${edges[@]}"; do
    node=${edge%%=*} ip=${edge#*=}
    for tenant in "${tenants[@]}"; do
      out=$(curl -sk -m 70 --resolve "$tenant.$domain:443:$ip" -w ' %{http_code} %{time_total}' "https://$tenant.$domain/value") || out="curl-error $?"
      took=${out##* }
      if [[ $out == "value of $tenant 200 "* ]]; then
        ok=$((ok + 1)); times+=("$took")
        if awk -v t="$took" 'BEGIN{exit !(t>1)}'; then
          slow=$((slow + 1)); echo "$(date +%T.%3N) slow $node -> $tenant: ${took}s" >&2
        fi
      else
        failed=$((failed + 1)); echo "$(date +%T.%3N) FAILED $node -> $tenant: $out" >&2
      fi
    done
  done
done
latency=$(printf '%s\n' "${times[@]}" | sort -n | awk '{a[NR]=$1} END {if (NR) {p=int(NR*0.99)+1; if (p>NR) p=NR; printf "p50=%.0fms p99=%.0fms max=%.0fms", a[int(NR/2)+1]*1000, a[p]*1000, a[NR]*1000}}')
echo "ok=$ok failed=$failed slow(>1s)=$slow $latency"
