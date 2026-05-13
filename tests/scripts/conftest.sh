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
    local repo
    repo=$(mktemp -d)
    git -C "$repo" init -q --initial-branch=main
    git -C "$repo" config user.email test@example.com
    git -C "$repo" config user.name "Test User"
    echo "initial" > "$repo/file.txt"
    git -C "$repo" add file.txt
    git -C "$repo" commit -q -m "initial"

    if [[ " $* " == *" --pushed "* ]]; then
        local remote
        remote=$(mktemp -d)
        git -C "$remote" init -q --bare
        git -C "$repo" remote add origin "$remote"
        git -C "$repo" push -q origin main
    fi

    if [[ " $* " == *" --dirty "* ]]; then
        echo "uncommitted" > "$repo/file.txt"
    fi

    echo "$repo"
}

# Cleanup is via bats' default tmpdir handling.
