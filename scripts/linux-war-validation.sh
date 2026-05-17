#!/usr/bin/env bash
set -euo pipefail

export RUN_RUNTIME_LOAD_TEST=true
export RUN_REPLAY_HARNESS=true
export REPLAY_AUTO_TUNE=true
export REPLAY_AUTO_TUNE_APPLY=true
export REPLAY_AUTO_TUNE_USE_IN_RUNTIME=true
export FREEZE_REFERENCE_ARTIFACTS=true

NETWORK="${NETWORK:-polygon}"
REPLAY_INPUT_PATH="${REPLAY_INPUT_PATH:-./replay/polygon_cases.jsonl}"
REPLAY_TUNE_THRESHOLD_MULTIPLIERS="${REPLAY_TUNE_THRESHOLD_MULTIPLIERS:-0.90,1.00,1.10}"
REPLAY_TUNE_PRIORITY_SHIFTS="${REPLAY_TUNE_PRIORITY_SHIFTS:--0.05,0.00,0.05}"
REPLAY_TUNE_TOXICITY_SHIFTS="${REPLAY_TUNE_TOXICITY_SHIFTS:--0.05,0.00,0.05}"
REPLAY_TUNE_GAS_EXTRA_BPS="${REPLAY_TUNE_GAS_EXTRA_BPS:-0,500,1000,2000}"

echo "[war-validation] baseline hot-gate"
RUNTIME_LOAD_TEST_PROFILE=baseline cargo run --release -- --network "${NETWORK}"

echo "[war-validation] adversarial hot-gate"
RUNTIME_LOAD_TEST_PROFILE=adversarial cargo run --release -- --network "${NETWORK}"

echo "[war-validation] replay auto-tune"
unset RUN_RUNTIME_LOAD_TEST
cargo run --release -- --network "${NETWORK}"

echo "[war-validation] reference artifacts frozen under exports/reference"
