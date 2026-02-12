#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

JAR_PATH="$(ls kingbase-lite-console/target/kingbase-lite-console-*.jar 2>/dev/null | grep -v '\\.original$' | head -n 1 || true)"
if [[ -z "$JAR_PATH" ]]; then
  echo "No runnable jar found. Run: mvn -q -pl kingbase-lite-console -am -DskipTests package" >&2
  exit 1
fi

java -jar "$JAR_PATH"
