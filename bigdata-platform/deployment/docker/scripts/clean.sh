#!/bin/bash
set -e
cd "$(dirname "$0")/.."
echo "⚠️  这将删除所有数据！"
read -p "确认? (yes): " c
[ "$c" != "yes" ] && echo "取消" && exit 0
docker compose down -v
echo "✅ 已清理"
