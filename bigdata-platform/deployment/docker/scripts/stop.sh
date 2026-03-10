#!/bin/bash
set -e
cd "$(dirname "$0")/.."
echo "🛑 停止大数据平台..."
docker compose down
echo "✅ 已停止。数据卷已保留，如需清理: ./scripts/clean.sh"
