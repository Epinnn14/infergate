#!/usr/bin/env bash
# Point a copy of this repository at its new home: Go module path, container images, chart and
# Argo CD links, badges.
#   scripts/use-template.sh <github-owner> [repo-name]
#   scripts/use-template.sh acme-ai                  # → github.com/acme-ai/infergate, ghcr.io/acme-ai/infergate-*
#   scripts/use-template.sh acme-ai llm-gateway      # → github.com/acme-ai/llm-gateway
# A repository created with "Use this template" on GitHub runs this once by itself
# (.github/workflows/template-init.yaml). Image names stay infergate-gateway / -worker / -registry.
set -euo pipefail
cd "$(dirname "$0")/.."

owner=${1:?usage: scripts/use-template.sh <github-owner> [repo-name]}
repo=${2:-infergate}
lower=$(printf '%s' "$owner" | tr '[:upper:]' '[:lower:]')
from_owner=Epinnn14 from_repo=infergate

case "$owner/$repo" in
  */*/*|*" "*) echo "owner and repo must be plain names" >&2; exit 1 ;;
esac

# text files tracked by git that mention the template's home (this script and the workflow keep it)
files=$(git ls-files | grep -v -e '^scripts/use-template.sh$' -e '^.github/workflows/template-init.yaml$' \
  | xargs grep -Il -e "$from_owner/$from_repo" -e "ghcr.io/$(printf '%s' "$from_owner" | tr '[:upper:]' '[:lower:]')" 2>/dev/null || true)
if [ -z "$files" ]; then echo "nothing to rename (already done?)"; exit 0; fi

for f in $files; do
  sed -i.bak \
    -e "s#github.com/$from_owner/$from_repo#github.com/$owner/$repo#g" \
    -e "s#$from_owner/$from_repo#$owner/$repo#g" \
    -e "s#ghcr\.io/epinnn14#ghcr.io/$lower#g" \
    "$f" && rm -f "$f.bak"
done
echo "updated:"; printf '  %s\n' $files

if command -v gofmt >/dev/null; then gofmt -w cmd internal; fi
if command -v go >/dev/null; then go build ./... && echo "go build ok"; fi
