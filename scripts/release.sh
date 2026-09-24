#!/bin/bash
# Release buffr: checks, tag, and the multi-arch image build on GitHub Actions.
#
# Usage:
 #   VERSION=0.8.10 ./scripts/release.sh [--dry-run] [--publish] [--force]
#
# Without --publish only the checks run. --publish pushes the commit and the tag
# and waits for the docker.yml run that builds and pushes
 # ghcr.io/robinbially/buffr for linux/amd64 and linux/arm64. --force continues
 # although the working tree is dirty.
#
# A local image build is deliberately not part of this script: a multi-arch
# build needs buildx and QEMU, which the CI runner provides.
#
# Environment:
#   VERSION             required, x.y.z
#   RELEASE_REPOSITORY  default RobinBially/buffr
#   IMAGE               default ghcr.io/robinbially/buffr
#   WORKFLOW            default docker.yml
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION="${VERSION:?VERSION must be set (x.y.z)}"
RELEASE_REPOSITORY="${RELEASE_REPOSITORY:-RobinBially/buffr}"
IMAGE="${IMAGE:-ghcr.io/robinbially/buffr}"
WORKFLOW="${WORKFLOW:-docker.yml}"

[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "VERSION must be x.y.z, got: $VERSION" >&2; exit 1; }

dry_run=0
publish=0
force=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) dry_run=1; shift ;;
        --publish) publish=1; shift ;;
        --force) force=1; shift ;;
        -h|--help) sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

# --- Voraussetzungen ---------------------------------------------------------
issues=()
command -v go >/dev/null || issues+=("go fehlt.")
command -v gh >/dev/null || issues+=("gh CLI fehlt.")
if [[ $force -eq 0 ]]; then
    git diff --quiet || issues+=("Uncommitted changes; erst committen (--force überspringt).")
    [[ -z "$(git ls-files --others --exclude-standard)" ]] || issues+=("Untracked files; erst committen oder ignorieren (--force überspringt).")
fi
git rev-parse -q --verify "refs/tags/v$VERSION" >/dev/null && issues+=("Tag v$VERSION existiert lokal schon.")
git ls-remote --exit-code --tags origin "refs/tags/v$VERSION" >/dev/null 2>&1 && issues+=("Tag v$VERSION ist im Remote schon vorhanden.")

upstream="$(git rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null || true)"
if [[ -n "$upstream" ]]; then
    git fetch --quiet origin || issues+=("git fetch origin ist fehlgeschlagen.")
    behind="$(git rev-list --count "HEAD..$upstream" 2>/dev/null || echo 0)"
    [[ "$behind" == 0 ]] || issues+=("$behind Commit(s) fehlen lokal gegenüber $upstream; erst pullen.")
fi

if [[ ${#issues[@]} -gt 0 ]]; then
    echo "Voraussetzungen nicht erfüllt:" >&2
    printf "  - %s\n" "${issues[@]}" >&2
    exit 1
fi

echo "== buffr $VERSION"
echo "   Repository $RELEASE_REPOSITORY"
echo "   Image      $IMAGE:$VERSION (plus :latest, :sha)"
if [[ $publish -eq 1 ]]; then echo "   Modus      veröffentlichen"; else echo "   Modus      nur Checks (--publish veröffentlicht)"; fi

if [[ $dry_run -eq 1 ]]; then
    echo "== Probelauf: Voraussetzungen erfüllt, nichts gebaut."
    exit 0
fi

# --- Checks ------------------------------------------------------------------
echo "== Checks"
go test ./...
go build ./...

if [[ $publish -eq 0 ]]; then
    cat <<EOF
== Fertig (nur Checks)
   Das Image baut CI: erneut mit --publish starten, dann wird der Tag
   v$VERSION gepusht und $WORKFLOW gebaut.
EOF
    exit 0
fi

# --- Tag und Image -----------------------------------------------------------
echo "== Tag und Image"
SOURCE_COMMIT="$(git rev-parse HEAD)"
git tag "v$VERSION"
git push origin HEAD
git push origin "v$VERSION"
echo "   Tag gepusht, warte auf $WORKFLOW ..."
run_id=""
for _ in $(seq 1 30); do
    run_id="$( gh run list --repo "$RELEASE_REPOSITORY" --workflow "$WORKFLOW" --limit 20 \
        --json databaseId,headBranch \
        --jq "[.[] | select(.headBranch == \"v$VERSION\")][0].databaseId" 2>/dev/null || true )"
    if [[ -n "$run_id" && "$run_id" != "null" ]]; then break; fi
    sleep 10
done
[[ -n "$run_id" && "$run_id" != "null" ]] || { echo "Kein $WORKFLOW-Lauf für v$VERSION gefunden." >&2; exit 1; }
gh run watch "$run_id" --repo "$RELEASE_REPOSITORY" --exit-status >/dev/null

cat <<EOF
== Fertig
   Image    $IMAGE:$VERSION
   Tag      https://github.com/$RELEASE_REPOSITORY/releases/tag/v$VERSION
   Lauf     https://github.com/$RELEASE_REPOSITORY/actions/runs/$run_id
   Quelle   $SOURCE_COMMIT
EOF
