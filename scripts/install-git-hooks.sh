#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"

git -C "$repo_root" config core.hooksPath .githooks
chmod +x "$repo_root/.githooks/pre-commit"

echo "Installed git hooks path: $repo_root/.githooks"
echo "pre-commit will now run cargo fmt and cargo clippy."

