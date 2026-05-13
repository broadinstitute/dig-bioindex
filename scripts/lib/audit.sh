#!/usr/bin/env bash
# Audit invariants for deploys: ensure the local working tree being
# deployed represents a commit that is reachable on origin.
#
# Sourced by scripts that take a --local path (deploy.sh, build-base.sh).

# verify_clean_and_pushed <repo-path>
# Returns 0 if:
#   * working tree at <repo-path> has no uncommitted or unstaged changes
#   * HEAD's SHA exists on the origin remote (i.e., was pushed)
# Returns non-zero with an explanatory message on stderr otherwise.
verify_clean_and_pushed() {
    local repo="$1"
    if [[ ! -d "$repo/.git" ]]; then
        echo "ERROR: $repo is not a git repository" >&2
        return 1
    fi

    # Check working tree cleanliness
    if [[ -n "$(git -C "$repo" status --porcelain)" ]]; then
        echo "ERROR: working tree at $repo has uncommitted changes." >&2
        echo "       Run 'git status' in $repo. Commit and push before deploying." >&2
        return 1
    fi

    # Check HEAD is on origin
    local head_sha
    head_sha=$(git -C "$repo" rev-parse HEAD)
    if ! git -C "$repo" ls-remote origin 2>/dev/null | grep -q "^$head_sha"; then
        echo "ERROR: HEAD ($head_sha) is not on origin." >&2
        echo "       Push HEAD to origin before deploying." >&2
        return 1
    fi

    return 0
}
