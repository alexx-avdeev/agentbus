#!/usr/bin/env bash
# Install the agentbus CLI and the Claude Code skill on this machine.
# Usage: ./install.sh            (from a clone of the repo)
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

command -v cargo >/dev/null || {
  echo "cargo not found; install Rust first: https://rustup.rs" >&2; exit 1; }

echo "== building and installing agentbus binary"
cargo install --path "$here" --locked

skills="${CLAUDE_SKILLS_DIR:-$HOME/.claude/skills}"
mkdir -p "$skills"
if [ -e "$skills/agentbus" ] && [ ! -L "$skills/agentbus" ]; then
  echo "$skills/agentbus exists and is not a symlink; leaving it alone" >&2
else
  ln -sfn "$here/skill" "$skills/agentbus"
  echo "== skill linked: $skills/agentbus -> $here/skill"
fi

echo "== done"
echo "binary: $(command -v agentbus) ($(agentbus --version))"
echo "set AGENTBUS_PROJECT, AGENTBUS_AGENT and AMQP_URL, then run: agentbus join --role ..."
