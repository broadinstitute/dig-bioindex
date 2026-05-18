#!/usr/bin/env bash
# Audit invariants for deploys: ensure the local working tree being
# deployed represents a commit that is reachable on origin.
#
# Sourced by scripts that take a --local path (deploy.sh, build-base.sh).

# verify_clean_and_pushed <repo-path>
# Returns 0 if all three conditions hold:
#   * working tree at <repo-path> has no modifications to tracked files
#     (staged or unstaged). Untracked files are allowed — .dockerignore
#     plus the Dockerfile's targeted COPYs determine what actually
#     enters the image.
#   * an 'origin' remote is configured.
#   * HEAD's SHA exists on origin (i.e., was pushed).
# Returns non-zero with an explanatory message on stderr otherwise.
verify_clean_and_pushed() {
    local repo="$1"
    if [[ ! -d "$repo/.git" ]]; then
        echo "ERROR: $repo is not a git repository" >&2
        return 1
    fi

    # Check working tree cleanliness for tracked files only.
    # `git diff --quiet HEAD` catches both staged and unstaged
    # modifications. Untracked files are intentionally allowed.
    if ! git -C "$repo" diff --quiet HEAD 2>/dev/null; then
        echo "ERROR: working tree at $repo has uncommitted modifications to tracked files." >&2
        echo "       Run 'git status' in $repo. Commit and push before deploying." >&2
        return 1
    fi

    # Check origin exists at all (distinct from "HEAD not pushed")
    if ! git -C "$repo" remote get-url origin >/dev/null 2>&1; then
        echo "ERROR: no remote named 'origin' is configured in $repo." >&2
        echo "       Add origin and push HEAD before deploying." >&2
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
