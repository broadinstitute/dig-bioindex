load 'conftest'

setup() {
    AUDIT_LIB="$BATS_TEST_DIRNAME/../../scripts/lib/audit.sh"
    [ -f "$AUDIT_LIB" ] || skip "audit.sh does not exist yet"
    source "$AUDIT_LIB"
}

@test "verify_clean_and_pushed: passes for clean + pushed repo" {
    repo=$(make_test_repo --pushed)
    run verify_clean_and_pushed "$repo"
    [ "$status" -eq 0 ]
}

@test "verify_clean_and_pushed: fails on dirty working tree" {
    repo=$(make_test_repo --pushed --dirty)
    run verify_clean_and_pushed "$repo"
    [ "$status" -ne 0 ]
    [[ "$output" == *"uncommitted"* || "$output" == *"clean"* ]]
}

@test "verify_clean_and_pushed: fails when HEAD is not on origin" {
    repo=$(make_test_repo)  # no --pushed; no origin remote
    run verify_clean_and_pushed "$repo"
    [ "$status" -ne 0 ]
    [[ "$output" == *"origin"* || "$output" == *"pushed"* ]]
}

@test "verify_clean_and_pushed: fails when local has commits not on origin" {
    repo=$(make_test_repo --pushed)
    # Add a commit locally; do NOT push.
    echo "another" > "$repo/file2.txt"
    git -C "$repo" add file2.txt
    git -C "$repo" commit -q -m "another"
    run verify_clean_and_pushed "$repo"
    [ "$status" -ne 0 ]
    [[ "$output" == *"origin"* || "$output" == *"pushed"* ]]
}
