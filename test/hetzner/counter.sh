#!/usr/bin/env bash
# Writes 1, 2, 3, ... to a tenant's value through the edges in turn, five
# times a second, for SECONDS. Each outage is reported with its length;
# after one, the value that survived is read first: the acknowledged writes
# missing from it were lost. nodes.sh runs it on the client server.
#
#   counter.sh SECONDS DOMAIN TENANT "node=ip ..."
set -uo pipefail

seconds=$1 domain=$2 tenant=$3
read -ra edges <<< "$4"
url="https://$tenant.$domain/value"
now() { date +%s.%3N; }
acked=0 written=0 failures=0 down_since="" longest=0 lost_total=0
end=$((SECONDS + seconds))
i=0
while ((SECONDS < end)); do
  edge=${edges[$((i % ${#edges[@]}))]}; i=$((i + 1))
  ip=${edge#*=}
  if [[ -n $down_since ]]; then
    # Back? Read what survived before writing on.
    survived=$(curl -sk -m 5 --resolve "$tenant.$domain:443:$ip" "$url") || survived=""
    if [[ $survived =~ ^[0-9]+$ ]]; then
      gap=$(awk -v a="$down_since" -v b="$(now)" 'BEGIN{printf "%.1f", b-a}')
      lost=$((acked - survived)); ((lost < 0)) && lost=0
      lost_total=$((lost_total + lost))
      echo "$(date +%T.%3N) back after ${gap}s via ${edge%%=*}: value $survived, last acknowledged $acked, lost $lost"
      awk -v g="$gap" -v l="$longest" 'BEGIN{exit !(g>l)}' && longest=$gap
      down_since=""
    else
      sleep 0.2; continue
    fi
  fi
  next=$((acked + 1))
  code=$(curl -sk -m 5 -o /dev/null -w '%{http_code}' -X PUT --data "$next" --resolve "$tenant.$domain:443:$ip" "$url") || code="curl-error"
  written=$((written + 1))
  if [[ $code == 204 ]]; then
    acked=$next
  else
    failures=$((failures + 1))
    if [[ -z $down_since ]]; then
      down_since=$(now)
      echo "$(date +%T.%3N) down via ${edge%%=*}: $code after acknowledging $acked"
    fi
  fi
  sleep 0.2
done
echo "acknowledged=$acked failed=$failures longest_outage=${longest}s lost=$lost_total"
