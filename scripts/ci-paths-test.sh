#!/bin/sh
# Both sides of the path list in scripts/ci-paths.sh, the pull-request mode against a real merge
# commit, and the audit of this tree. Run by .github/workflows/planning.yml on every pull request.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
list="$here/ci-paths.sh"
failures=0
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

check() {
    name=$1
    expected=$2
    actual=$3
    if [ "$actual" = "$expected" ]; then
        echo "ok   $name"
    else
        echo "FAIL $name: expected [$expected], got [$actual]"
        failures=$((failures + 1))
    fi
}

classified() {
    printf '%s\n' "$@" | sh "$list" paths 2>/dev/null | tr '\n' ' '
}

check "a planning-only change set runs no build or test" "code=false docs=false " \
    "$(classified .engineering/planning/story/a.md .engineering/evidence/run.json \
        .engineering/project.yaml docs/stories/README.md docs/design/file-provider.md)"
check "the same set with one code path runs everything" "code=true docs=true " \
    "$(classified .engineering/planning/story/a.md docs/design/file-provider.md \
        crates/eventlog-core/src/lib.rs)"
check "a change to the built site runs the site build only" "code=false docs=true " \
    "$(classified website/docusaurus.config.js .engineering/planning/story/a.md)"
check "Markdown a test reads is code" "code=true docs=true " "$(classified README.md)"
check "Markdown inside a crate is code" "code=true docs=true " \
    "$(classified crates/eventlog-core/README.md)"
check "a workflow change is code" "code=true docs=true " \
    "$(classified .github/workflows/planning.yml)"
check "an empty change list runs everything" "code=true docs=true " "$(printf '' | sh "$list" paths 2>/dev/null | tr '\n' ' ')"

# A repository with a base commit, a pull request head and the merge commit a pull_request
# workflow checks out. Plumbing only, so no hook or signing configuration takes part.
repo="$work/repo"
git init -q "$repo"
export GIT_AUTHOR_NAME=ci-paths GIT_AUTHOR_EMAIL=ci-paths@invalid
export GIT_COMMITTER_NAME=ci-paths GIT_COMMITTER_EMAIL=ci-paths@invalid
commit() {
    git -C "$repo" add -A
    git -C "$repo" commit-tree "$(git -C "$repo" write-tree)" "$@" -m fixture
}
mkdir -p "$repo/crates/a" "$repo/.engineering"
echo code >"$repo/crates/a/lib.rs"
echo plan >"$repo/.engineering/a.md"
base=$(commit)
echo more >>"$repo/.engineering/a.md"
planning_head=$(commit -p "$base")
planning_merge=$(commit -p "$base" -p "$planning_head")
git -C "$repo" mv crates/a/lib.rs .engineering/lib.rs
moved_head=$(commit -p "$base")
moved_merge=$(commit -p "$base" -p "$moved_head")

at() {
    git -C "$repo" update-ref --no-deref HEAD "$1"
    (cd "$repo" && GITHUB_EVENT_NAME=$2 GITHUB_OUTPUT='' sh "$list" pull-request 2>/dev/null |
        tr '\n' ' ')
}
check "a planning-only pull request merge commit runs no build or test" "code=false docs=false " \
    "$(at "$planning_merge" pull_request)"
check "moving a code file into the planning store is a code change" "code=true docs=true " \
    "$(at "$moved_merge" pull_request)"
check "a push runs everything" "code=true docs=true " "$(at "$planning_merge" push)"
check "a checkout that is not a merge commit runs everything" "code=true docs=true " \
    "$(at "$planning_head" pull_request)"

output="$work/github-output"
: >"$output"
(cd "$repo" && git update-ref --no-deref HEAD "$planning_merge" &&
    GITHUB_EVENT_NAME=pull_request GITHUB_OUTPUT="$output" sh "$list" pull-request >/dev/null 2>&1)
check "the result reaches the step outputs" "code=false docs=false " "$(tr '\n' ' ' <"$output")"

if sh "$list" audit; then
    echo "ok   no file the list calls prose is named by code"
else
    echo "FAIL the audit found prose that code names"
    failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
    echo "ci-paths-test: $failures failure(s)"
    exit 1
fi
echo "ci-paths-test: all cases passed"
