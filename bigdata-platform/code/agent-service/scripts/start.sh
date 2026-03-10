#!/bin/bash
# 一键启动脚本
set -e

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"

echo "🤖 Data Analysis Agent"
echo "========================"

# 检查 .env
if [ ! -f .env ]; then
    echo "⚠️  未找到 .env 文件，从模板创建..."
    cp .env.example .env
    echo "📝 请编辑 .env 文件，填入 API Key 和数据库连接信息"
    echo "   vim .env"
    exit 1
fi

# 检查 Python
if ! command -v python3 &>/dev/null; then
    echo "❌ 未找到 python3，请先安装"
    exit 1
fi

# 创建虚拟环境
if [ ! -d venv ]; then
    echo "📦 创建虚拟环境..."
    python3 -m venv venv
fi
source venv/bin/activate

# 安装依赖
echo "📦 安装依赖..."
pip install -r requirements.txt -q

# 创建输出目录
mkdir -p output/charts output/reports

# 启动模式
MODE=${1:-cli}

case $MODE in
    cli)
        echo "🖥️  启动命令行模式..."
        python main.py
        ;;
    web)
        echo "🌐 启动 Web 服务..."
        python web/app.py
        ;;
    query)
        shift
        echo "🔍 单次查询: $*"
        python main.py "$@"
        ;;
    test)
        echo "🧪 运行测试..."
        pip install pytest -q
        python -m pytest tests/ -v
        ;;
    *)
        echo "用法: $0 {cli|web|query|test}"
        echo "  cli   - 交互式命令行（默认）"
        echo "  web   - 启动 Web 服务"
        echo "  query - 单次查询，如: $0 query '昨天的GMV是多少'"
        echo "  test  - 运行测试"
        exit 1
        ;;
esac
