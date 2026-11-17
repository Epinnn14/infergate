#!/usr/bin/env bash
# Point a copy of this repository at its new home: Go module path, container images, chart and
# Argo CD links, badges.
#   scripts/use-template.sh <github-owner> [repo-name]
#   scripts/use-template.sh acme-ai                  # → github.com/acme-ai/infergate, ghcr.io/acme-ai/infergate-*
#   scripts/use-template.sh acme-ai llm-gateway      # → github.com/acme-ai/llm-gateway, ghcr.io/acme-ai/llm-gateway-*
# A repository created with "Use this template" on GitHub runs this once by itself
# (.github/workflows/template-init.yaml). Images are named after the repository, as CI publishes
# them: <repo>-gateway, <repo>-worker, <repo>-registry.
set -euo pipefail
cd "$(dirname "$0")/.."

owner=${1:?usage: scripts/use-template.sh <github-owner> [repo-name]}
repo=${2:-infergate}
lower=$(printf '%s' "$owner" | tr '[:upper:]' '[:lower:]')
repo_lc=$(printf '%s' "$repo" | tr '[:upper:]' '[:lower:]')
from_owner=Epinnn14 from_repo=infergate

case "$owner/$repo" in
  */*/*|*" "*) echo "owner and repo must be plain names" >&2; exit 1 ;;
esac

# text files tracked by git that mention the template's home (this script and the workflow keep it)
files=$(git ls-files | grep -v -e '^scripts/use-template.sh$' -e '^.github/workflows/template-init.yaml$' \
  | xargs grep -Il -e "$from_owner/$from_repo" -e "ghcr.io/$(printf '%s' "$from_owner" | tr '[:upper:]' '[:lower:]')" 2>/dev/null || true)
if [ -z "$files" ]; then echo "nothing to rename (already done?)"; exit 0; fi

for f in $files; do
  # one rule per name, and only where the old name ends: "infergate" must not match inside
  # "infergate-test", or renaming Epinnn14/infergate to Epinnn14/infergate-test would repeat
  sed -E -i.bak \
    -e "s#$from_owner/$from_repo([^-A-Za-z0-9_]|\$)#$owner/$repo\1#g" \
    -e "s#ghcr\.io/epinnn14/infergate-(gateway|worker|registry)#ghcr.io/$lower/$repo_lc-\1#g" \
    -e "s#\"infergate\" (\# image-prefix)#\"$repo_lc\" \1#" \
    -e "s#ghcr\.io/epinnn14([^-A-Za-z0-9_]|\$)#ghcr.io/$lower\1#g" \
    "$f" && rm -f "$f.bak"
done
echo "updated:"; printf '  %s\n' $files

if command -v gofmt >/dev/null; then gofmt -w cmd internal; fi
if command -v go >/dev/null; then go build ./... && echo "go build ok"; fi
