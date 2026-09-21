#!/usr/bin/env bash
# Regenerates contracts/BenchTarget.hex (creation bytecode embedded into the gum-bench binary).
# Requires foundry (`forge`). Deterministic: pinned solc, optimizer on, no metadata hash.
set -euo pipefail
cd "$(dirname "$0")/.."
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/src"
cp contracts/BenchTarget.sol "$tmp/src/"
cat > "$tmp/foundry.toml" <<TOML
[profile.default]
src = "src"
out = "out"
solc = "0.8.30"
evm_version = "paris"
optimizer = true
optimizer_runs = 200
bytecode_hash = "none"
cbor_metadata = false
TOML
(cd "$tmp" && forge build --quiet)
(cd "$tmp" && forge inspect BenchTarget bytecode) | tr -d '\n' > contracts/BenchTarget.hex
echo "wrote contracts/BenchTarget.hex ($(wc -c < contracts/BenchTarget.hex | tr -d ' ') hex chars)"
