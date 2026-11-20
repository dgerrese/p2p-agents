#!/usr/bin/env bash
# Status line: SDD build progress + naive ETA from the ledger.
cd "$(dirname "$0")/.." || exit 0
L=.superpowers/sdd/2026-11-20-p2p-agents/progress.md
[ -f "$L" ] || { echo "p2p-agents"; exit 0; }
total=8
done=$(grep -cE '^Task [0-9]+: complete' "$L")
cur=$(grep -oE '^Task [0-9]+: (dispatched|fix round [0-9]+)' "$L" | tail -1)
n=${cur%%:*}; n=${n#Task }
state=building; [[ $cur == *fix* ]] && state=fixing
start=$(git log --reverse --format=%ct 2>/dev/null | head -1)
now=$(date +%s)
# ponytail: ETA = avg time per finished task x remaining; ignores that tasks 4-5 are bigger
if [ "$done" -gt 0 ] && [ -n "$start" ]; then
  eta=$(( (now - start) / done * (total - done) / 60 ))
  etas="ETA ~${eta}m ($(date -d "@$((now + eta*60))" +%H:%M))"
else etas="ETA --"; fi
[ "$done" -ge "$total" ] && { echo "p2p-agents: all $total tasks done"; exit 0; }
echo "p2p-agents: $done/$total done · task $n $state · $etas"
