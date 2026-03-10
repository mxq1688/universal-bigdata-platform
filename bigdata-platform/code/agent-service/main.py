#!/usr/bin/env python3
"""
Data Analysis Agent - 命令行入口
支持三种模式: 交互式对话 / 单次查询 / Web 服务
"""
import sys
import argparse
import logging
from pathlib import Path

# 项目根目录加入 path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from agent.core import DataAnalysisAgent

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("agent.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
for name in ("httpx", "httpcore", "openai", "anthropic", "urllib3", "sqlalchemy"):
    logging.getLogger(name).setLevel(logging.WARNING)


def print_banner():
    try:
        from rich.console import Console
        from rich.panel import Panel
        console = Console()
        console.print(Panel.fit(
            "[bold cyan]🤖 Data Analysis Agent[/bold cyan]\n"
            "[dim]自然语言 → SQL → 分析 → 图表 → 结论[/dim]\n"
            "[dim]输入 'quit' 或 'exit' 退出，输入 'help' 查看帮助[/dim]",
            border_style="cyan",
        ))
    except ImportError:
        print("=" * 50)
        print("🤖 Data Analysis Agent")
        print("自然语言 → SQL → 分析 → 图表 → 结论")
        print("输入 'quit' 或 'exit' 退出")
        print("=" * 50)


def print_result(result):
    """打印分析结果"""
    try:
        from rich.console import Console
        from rich.markdown import Markdown
        from rich.table import Table
        console = Console()
        _print_result_rich(console, result)
    except ImportError:
        _print_result_plain(result)


def _print_result_rich(console, result):
    from rich.markdown import Markdown
    from rich.table import Table

    if not result.success:
        console.print(f"\n[bold red]❌ 分析失败: {result.error}[/bold red]")
        return

    console.print()
    console.print(Markdown(result.summary))

    if result.charts:
        console.print()
        table = Table(title="📊 生成的图表", show_lines=True)
        table.add_column("#", style="cyan", width=4)
        table.add_column("文件路径", style="green")
        for i, chart in enumerate(result.charts, 1):
            table.add_row(str(i), chart)
        console.print(table)

    console.print(
        f"\n[dim]───── SQL: {result.sql_count} 次 | "
        f"图表: {len(result.charts)} 张 | "
        f"步骤: {len(result.steps)} 步 | "
        f"耗时: {result.elapsed_ms:.0f}ms ─────[/dim]"
    )


def _print_result_plain(result):
    if not result.success:
        print(f"\n❌ 分析失败: {result.error}")
        return

    print(f"\n{result.summary}")

    if result.charts:
        print("\n📊 生成的图表:")
        for i, chart in enumerate(result.charts, 1):
            print(f"  {i}. {chart}")

    print(f"\n───── SQL: {result.sql_count} 次 | "
          f"图表: {len(result.charts)} 张 | "
          f"步骤: {len(result.steps)} 步 | "
          f"耗时: {result.elapsed_ms:.0f}ms ─────")


def interactive_mode():
    """交互式对话模式"""
    print_banner()
    print("正在初始化 Agent...")

    try:
        agent = DataAnalysisAgent()
    except Exception as e:
        print(f"初始化失败: {e}")
        print("请检查 .env 配置文件中的 API Key 和数据库连接")
        return

    print("✅ Agent 已就绪！\n")

    while True:
        try:
            query = input("📝 你的问题 > ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n再见 👋")
            break

        if not query:
            continue
        if query.lower() in ("quit", "exit", "q"):
            print("再见 👋")
            break
        if query.lower() == "help":
            print("""
使用示例:
  • 昨天的 GMV 和订单量是多少？
  • 最近7天的日活趋势，画个折线图
  • 各城市交易额排名 TOP 10
  • 用户转化漏斗分析
  • 退款率最高的商品分类有哪些？
  • 对比上周和本周的销售额变化
""")
            continue

        print(f"\n🔄 正在分析...")
        result = agent.analyze(query)
        print_result(result)
        print()


def single_query(query: str):
    """单次查询模式"""
    agent = DataAnalysisAgent()
    result = agent.analyze(query)
    print_result(result)


def web_mode(host: str = "0.0.0.0", port: int = 8501):
    """Web 服务模式"""
    from web.app import start_server
    print(f"🌐 启动 Web 服务: http://{host}:{port}")
    start_server(host, port)


def main():
    parser = argparse.ArgumentParser(
        description="🤖 Data Analysis Agent - 自然语言驱动的数据分析",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py                        # 交互模式
  python main.py -q "昨天GMV多少？"      # 单次查询
  python main.py --web                   # Web 服务
  python main.py --web --port 9000       # 指定端口
""",
    )
    parser.add_argument("-q", "--query", type=str, help="单次查询模式，直接传入问题")
    parser.add_argument("--web", action="store_true", help="启动 Web 服务")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Web 服务绑定地址 (默认 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8501, help="Web 服务端口 (默认 8501)")

    args = parser.parse_args()

    if args.web:
        web_mode(args.host, args.port)
    elif args.query:
        single_query(args.query)
    else:
        interactive_mode()


if __name__ == "__main__":
    main()
