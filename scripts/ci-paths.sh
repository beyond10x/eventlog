#!/bin/sh
# Which CI work a change needs: the one path list of this repository.
#
# A pull request whose changed files are all planning (`.engineering/`) or prose (Markdown nothing
# builds or tests) needs no build, test, KPI or persistence proof. A change to the documentation
# site (`website/`) needs the site build and nothing else. Every other path is code and runs
# everything, and so do an empty change list and a change list git could not produce.
#
# Usage:
#   ci-paths.sh paths              classify the newline-separated paths on standard input
#   ci-paths.sh range BASE HEAD    classify `git diff BASE HEAD`
#   ci-paths.sh pull-request       classify the pull request a workflow checked out (its merge
#                                  commit against its first parent); any other event runs everything
#   ci-paths.sh audit              fail when a file this list calls prose is named by a code file
#
# Each classifying mode prints `code=true|false` and `docs=true|false` and appends both to
# `$GITHUB_OUTPUT` when that is set. Tested by scripts/ci-paths-test.sh.
set -eu

# The path list. First match wins; `class` is planning, prose, docs or code.
classify() {
    case $1 in
        .engineering/*) class=planning ;;
        # Read by crates/eventlog-file/tests/adversary_stream_index_docs.rs.
        README.md | CHANGELOG.md | website/docs/*) class=code ;;
        # Named by the path-length assertion in crates/eventlog-tree/src/layout.rs.
        docs/design/tree-layout-path-length-v0.1.md) class=code ;;
        website/*) class=docs ;;
        crates/* | ess/* | kpi/* | scripts/*) class=code ;;
        *.md) class=prose ;;
        *) class=code ;;
    esac
}

# What the audit searches: every file that is not Markdown, planning or the site, except the path
# lists themselves, whose tests name example paths (the KPI lane's own list is scope.rs).
scanned='. :!*.md :!.engineering :!website :!scripts/ci-paths.sh :!scripts/ci-paths-test.sh :!crates/eventlog-kpi/src/scope.rs'

emit() {
    printf 'code=%s\ndocs=%s\n' "$1" "$2"
    if [ -n "${GITHUB_OUTPUT:-}" ]; then
        printf 'code=%s\ndocs=%s\n' "$1" "$2" >>"$GITHUB_OUTPUT"
    fi
}

everything() {
    echo "ci-paths: $1; running everything" >&2
    emit true true
}

paths() {
    code=false
    docs=false
    count=0
    while IFS= read -r path || [ -n "$path" ]; do
        [ -n "$path" ] || continue
        count=$((count + 1))
        classify "$path"
        case $class in
            code) code=true docs=true ;;
            docs) docs=true ;;
        esac
    done
    if [ "$count" -eq 0 ]; then
        everything "no changed path"
        return
    fi
    echo "ci-paths: $count changed path(s)" >&2
    emit "$code" "$docs"
}

range() {
    # --no-renames lists a moved file under its old path too: moving code out is a code change.
    if ! changed=$(git -c core.quotePath=true diff --no-renames --name-only "$1" "$2" --); then
        everything "git diff $1 $2 failed"
        return
    fi
    printf '%s\n' "$changed" | paths
}

pull_request() {
    if [ "${GITHUB_EVENT_NAME:-}" != pull_request ]; then
        everything "event ${GITHUB_EVENT_NAME:-unset} is not a pull request"
        return
    fi
    if ! git rev-parse -q --verify 'HEAD^2' >/dev/null; then
        everything "HEAD is not the pull request's merge commit"
        return
    fi
    range 'HEAD^1' HEAD
}

# A file this list calls prose must be one no code reads. Every non-Markdown file outside the
# planning store and the site is searched for a quoted literal naming the file, its base name or a
# directory above it; a hit fails with the place that names it, and the path joins the code list.
# A path assembled at run time from pieces that are not such literals is beyond this search.
audit() {
    cd "$(git rev-parse --show-toplevel)"
    set -f
    work=$(mktemp -d)
    trap 'rm -rf "$work"' EXIT
    git -c core.quotePath=false ls-files >"$work/files"
    : >"$work/prose"
    while IFS= read -r path; do
        classify "$path"
        [ "$class" = prose ] && printf '%s\n' "$path" >>"$work/prose"
    done <"$work/files"
    # One line per prose file and search string: the file, a tab, the string.
    # The whole path (quoted, or after a slash as in `../README.md`), the base name after a slash,
    # and each directory above the file (quoted, or quoted and followed by a slash).
    awk -v q="'" '
        function both(path, before, name, after) {
            print path "\t" before "\"" name after; print path "\t" before q name q
        }
        {
            print $0 "\t\"" $0 "\""; print $0 "\t" q $0 q
            print $0 "\t/" $0 "\""; print $0 "\t/" $0 q
            n = split($0, part, "/")
            if (n > 1) { print $0 "\t/" part[n] "\""; print $0 "\t/" part[n] q }
            dir = ""
            for (i = 1; i < n; i++) {
                dir = (i == 1) ? part[1] : dir "/" part[i]
                print $0 "\t\"" dir "\""; print $0 "\t" q dir q
                print $0 "\t\"" dir "/"; print $0 "\t" q dir "/"
            }
        }' "$work/prose" >"$work/pairs"
    cut -f2 "$work/pairs" | sort -u >"$work/patterns"
    [ -s "$work/patterns" ] || return 0
    # shellcheck disable=SC2086 # $scanned is a list of pathspecs without spaces
    git grep -h -o -F -f "$work/patterns" -- $scanned | sort -u >"$work/found" || true
    awk -F '\t' 'NR == FNR { found[$0] = 1; next } ($2 in found) { print $1 "\t" $2 }' \
        "$work/found" "$work/pairs" >"$work/hits"
    if [ -s "$work/hits" ]; then
        while IFS="$(printf '\t')" read -r path pattern; do
            # shellcheck disable=SC2086
            where=$(git grep -n -F -e "$pattern" -- $scanned | head -n 1)
            echo "ci-paths: $path is prose in the path list, but code names it: $where" >&2
        done <"$work/hits"
        return 1
    fi
    echo "ci-paths: $(wc -l <"$work/prose" | tr -d ' ') prose file(s), none named by code" >&2
}

case ${1:-} in
    paths) paths ;;
    range) range "$2" "$3" ;;
    pull-request) pull_request ;;
    audit) audit ;;
    *)
        echo "usage: ci-paths.sh paths | range BASE HEAD | pull-request | audit" >&2
        exit 2
        ;;
esac
