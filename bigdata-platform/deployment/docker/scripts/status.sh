#!/bin/bash
cd "$(dirname "$0")/.."
echo "📊 大数据平台运行状态"
echo "========================"
docker compose ps
echo ""
echo "📊 资源使用:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" 2>/dev/null | head -25
