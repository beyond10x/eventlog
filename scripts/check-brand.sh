#!/usr/bin/env bash
# The former brand names (b10x, codewandler) are banned at the surface of
# this repository. The only remaining exemption is the b10x-bot GitHub App
# machinery: scripts/as-bot.sh, scripts/bot-token.sh and scripts/check-bot-files.py
# carry the App's own name and its B10X_BOT_* env vars, which are functional
# identifiers that can only be renamed together with the GitHub App itself.
# Provenance URLs into the old monorepo, the "the b10x monorepo" extraction
# phrase and CHANGELOG mentions are all gone: do not re-add exemptions for them.
set -euo pipefail
# The former brand, assembled at runtime: a guard that spells the banned string contiguously
# would itself be a hit. `printf` keeps the pattern out of the file while the check still works.
BANNED="$(printf 'daemon%sloom|codewandler' '')"
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
hits=$(git grep -inE '${BANNED}' -- \
  ':!scripts/check-brand.sh' \
  ':!scripts/as-bot.sh' ':!scripts/bot-token.sh' ':!scripts/check-bot-files.py' \
  || true)
if test -n "$hits"; then
  printf 'brand check: former brand name at the surface:\n%s\n' "$hits" >&2
  exit 1
fi
printf 'brand check: clean\n'
