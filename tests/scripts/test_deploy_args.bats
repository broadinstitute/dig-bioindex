load 'conftest'

setup() {
    DEPLOY="$BATS_TEST_DIRNAME/../../scripts/deploy.sh"
    [ -x "$DEPLOY" ] || skip "deploy.sh not yet present"
}

@test "deploy.sh: requires env argument" {
    run "$DEPLOY"
    [ "$status" -ne 0 ]
    [[ "$output" == *"Usage"* || "$output" == *"env"* ]]
}

@test "deploy.sh: rejects unknown env" {
    run "$DEPLOY" staging --branch master
    [ "$status" -ne 0 ]
    [[ "$output" == *"qa"* || "$output" == *"prod"* ]]
}

@test "deploy.sh: rejects more than one ref-form" {
    run "$DEPLOY" qa --branch master --tag v1
    [ "$status" -ne 0 ]
    [[ "$output" == *"one"* || "$output" == *"ref"* ]]
}

@test "deploy.sh: rejects --local on missing path" {
    run "$DEPLOY" qa --local /nonexistent/path
    [ "$status" -ne 0 ]
    [[ "$output" == *"not"* ]]
}

@test "deploy.sh: --dry-run is accepted as a flag" {
    run "$DEPLOY" qa --branch master --dry-run --help-after-parse
    [[ "$output" != *"unknown"*"dry-run"* ]]
}
