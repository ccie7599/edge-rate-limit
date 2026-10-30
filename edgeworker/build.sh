#!/usr/bin/env bash
# Build an EdgeWorker bundle: ./build.sh request|provider [version] [policies-file]
# Output: dist/rl-<variant>-<version>.tgz with main.js + bundle.json + modules.
set -euo pipefail
cd "$(dirname "$0")"
variant=${1:?usage: build.sh request|provider [version]}
version=${2:-0.1.0}
policies=${3:-src/policies.js}
out=dist/$variant
rm -rf "$out" && mkdir -p "$out"
cp src/rl.js src/jwt.js src/handlers.js "$out/"
cp "$policies" "$out/policies.js"
cp "src/main-$variant.js" "$out/main.js"
cat > "$out/bundle.json" <<JSON
{"edgeworker-version":"$version","description":"edge rate limit ($variant)"}
JSON
(cd "$out" && tar -czf "../rl-$variant-$version.tgz" main.js bundle.json rl.js jwt.js handlers.js policies.js)
echo "dist/rl-$variant-$version.tgz"
