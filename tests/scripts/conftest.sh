#!/usr/bin/env bash
# Shared bats helpers for scripts/ tests.
#
# Source this file at the top of each test:
#   load 'conftest'

# Create an ephemeral git repo with one commit, optionally pushed to a "remote".
# Usage:
#   make_test_repo                # creates clean repo, returns path on stdout
#   make_test_repo --pushed       # also creates a bare "remote" with HEAD pushed
#   make_test_repo --dirty        # leaves an uncommitted change in the working tree
make_test_repo() {
    local tmpbase="${BATS_TEST_TMPDIR:-$(mktemp -d -t bioindex-test.XXXXXX)}"
    local repo
    repo=$(mktemp -d "$tmpbase/repo.XXXXXX")
    git -C "$repo" init -q --initial-branch=main
    git -C "$repo" config user.email test@example.com
    git -C "$repo" config user.name "Test User"
    echo "initial" > "$repo/file.txt"
    git -C "$repo" add file.txt
    git -C "$repo" commit -q -m "initial"

    if [[ " $* " == *" --pushed "* ]]; then
        local remote
        remote=$(mktemp -d "$tmpbase/remote.XXXXXX")
        git -C "$remote" init -q --bare
        git -C "$repo" remote add origin "$remote"
        git -C "$repo" push -q origin main
    fi

    if [[ " $* " == *" --dirty "* ]]; then
        echo "uncommitted" > "$repo/file.txt"
    fi

    echo "$repo"
}

# When invoked under bats, $BATS_TEST_TMPDIR is per-test and auto-cleaned.
# When invoked directly (manual harness), the outer mktemp -d creates a
# fallback parent dir that the caller is responsible for cleaning.
