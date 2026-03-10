"""
数据分析 Agent 核心引擎
ReAct 循环: 推理 → 工具调用 → 观察 → 推理 → ... → 最终结论
"""
import json
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field

import pandas as pd

from agent import load_config, AgentConfig
from agent.llm import LLM
from agent.tools import TOOL_DEFINITIONS
from agent.tools.sql_tool import SQLTool
from agent.tools.pandas_tool import PandasTool
from agent.tools.chart_tool import ChartTool
from agent.knowledge.base import KnowledgeBase
from agent.prompts.system import build_system_prompt

logger = logging.getLogger(__name__)


@dataclass
class AnalysisResult:
    """单次分析的完整结果"""
    query: str
    summary: str = ""
    charts: list[str] = field(default_factory=list)
    steps: list[dict] = field(default_factory=list)
    sql_count: int = 0
    elapsed_ms: float = 0
    success: bool = True
    error: str | None = None


class DataAnalysisAgent:
    """
    数据分析 Agent
    用户输入自然语言问题 → Agent 自主规划 → 查询数据 → 分析 → 生成图表 → 输出结论
    """

    def __init__(self, llm_config=None, db_config=None, agent_config=None):
        # 加载配置
        _llm_cfg, _db_cfg, _agent_cfg = load_config()
        llm_config = llm_config or _llm_cfg
        db_config = db_config or _db_cfg
        agent_config = agent_config or _agent_cfg

        # 初始化组件
        self.llm = LLM(llm_config)
        self.sql_tool = SQLTool(db_config, agent_config)
        self.pandas_tool = PandasTool()
        self.chart_tool = ChartTool(agent_config.chart_output_dir)
        self.agent_config = agent_config

        # 知识库
        self.kb = KnowledgeBase()
        self.knowledge = self.kb.data

        # System Prompt
        self.system_prompt = build_system_prompt(
            self.knowledge,
            agent_config.max_sql_executions,
        )

        # 运行状态（每次 analyze 重置）
        self._dataframes: dict[str, pd.DataFrame] = {}
        self._charts: list[str] = []
        self._steps: list[dict] = []
        self._sql_count: int = 0

        logger.info(f"Agent 初始化完成 (LLM={llm_config.provider}/{llm_config.model})")

    # ================================================================
    #  主入口
    # ================================================================

    def analyze(self, user_query: str) -> AnalysisResult:
        """
        分析入口 —— 接收自然语言问题，返回完整分析结果
        """
        start = datetime.now()

        # 重置状态
        self._dataframes = {}
        self._charts = []
        self._steps = []
        self._sql_count = 0

        result = AnalysisResult(query=user_query)

        # 补充时间上下文
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        enriched_query = (
            f"用户问题: {user_query}\n"
            f"（今天是 {today}，昨天是 {yesterday}）"
        )

        # 对话消息列表
        messages = [{"role": "user", "content": enriched_query}]

        try:
            # ReAct 循环（最多 15 轮，防止死循环）
            for turn in range(15):
                logger.info(f"=== Agent Turn {turn + 1} ===")

                # 调用 LLM（带工具）
                response = self.llm.chat_with_tools(
                    self.system_prompt, messages, TOOL_DEFINITIONS
                )

                # 有文字输出就记录
                if response["content"]:
                    self._steps.append({
                        "type": "thought",
                        "content": response["content"],
                    })

                # 没有工具调用 → 分析结束
                if not response["tool_calls"]:
                    logger.info("Agent 结束（无工具调用）")
                    result.summary = response["content"]
                    break

                # 处理工具调用
                tool_results_for_llm = []
                for tool_call in response["tool_calls"]:
                    tool_name = tool_call["name"]
                    tool_args = tool_call["arguments"]
                    tool_id = tool_call.get("id", "")

                    logger.info(f"工具调用: {tool_name}({json.dumps(tool_args, ensure_ascii=False)[:200]})")

                    # 执行工具
                    tool_result = self._dispatch_tool(tool_name, tool_args)

                    # 记录步骤
                    self._steps.append({
                        "type": "tool_call",
                        "tool": tool_name,
                        "args": tool_args,
                        "result_summary": tool_result[:500] if isinstance(tool_result, str) else str(tool_result)[:500],
                    })

                    tool_results_for_llm.append({
                        "id": tool_id,
                        "name": tool_name,
                        "result": tool_result,
                    })

                    # 如果是 finish_analysis，直接结束
                    if tool_name == "finish_analysis":
                        result.summary = tool_args.get("summary", "")
                        result.charts = tool_args.get("charts", self._charts)
                        break

                # finish 了就跳出
                if result.summary:
                    break

                # 将工具结果追加到消息中（OpenAI 格式）
                if self.llm.provider == "openai":
                    # assistant message with tool_calls
                    assistant_msg = {"role": "assistant", "content": response["content"] or None}
                    if response["tool_calls"]:
                        assistant_msg["tool_calls"] = [
                            {
                                "id": tc["id"],
                                "type": "function",
                                "function": {
                                    "name": tc["name"],
                                    "arguments": json.dumps(tc["arguments"], ensure_ascii=False),
                                },
                            }
                            for tc in response["tool_calls"]
                        ]
                    messages.append(assistant_msg)

                    for tr in tool_results_for_llm:
                        messages.append({
                            "role": "tool",
                            "tool_call_id": tr["id"],
                            "content": tr["result"] if isinstance(tr["result"], str) else json.dumps(tr["result"], ensure_ascii=False, default=str),
                        })

                else:
                    # Anthropic 格式
                    assistant_content = []
                    if response["content"]:
                        assistant_content.append({"type": "text", "text": response["content"]})
                    for tc in response["tool_calls"]:
                        assistant_content.append({
                            "type": "tool_use",
                            "id": tc["id"],
                            "name": tc["name"],
                            "input": tc["arguments"],
                        })
                    messages.append({"role": "assistant", "content": assistant_content})

                    tool_result_content = []
                    for tr in tool_results_for_llm:
                        content = tr["result"] if isinstance(tr["result"], str) else json.dumps(tr["result"], ensure_ascii=False, default=str)
                        tool_result_content.append({
                            "type": "tool_result",
                            "tool_use_id": tr["id"],
                            "content": content,
                        })
                    messages.append({"role": "user", "content": tool_result_content})

        except Exception as e:
            logger.error(f"Agent 分析异常: {e}", exc_info=True)
            result.success = False
            result.error = str(e)

        # 汇总
        elapsed = (datetime.now() - start).total_seconds() * 1000
        result.steps = self._steps
        result.charts = self._charts
        result.sql_count = self._sql_count
        result.elapsed_ms = round(elapsed, 2)

        logger.info(f"分析完成: SQL={self._sql_count}次, 图表={len(self._charts)}张, 耗时={elapsed:.0f}ms")
        return result

    # ================================================================
    #  工具分发
    # ================================================================

    def _dispatch_tool(self, name: str, args: dict) -> str:
        """根据工具名分发到对应处理函数"""
        handlers = {
            "execute_sql": self._handle_sql,
            "run_pandas_code": self._handle_pandas,
            "generate_chart": self._handle_chart,
            "lookup_metric": self._handle_lookup_metric,
            "finish_analysis": self._handle_finish,
        }
        handler = handlers.get(name)
        if not handler:
            return f"未知工具: {name}"
        return handler(args)

    def _handle_sql(self, args: dict) -> str:
        sql = args["sql"]

        # 执行次数限制
        if self._sql_count >= self.agent_config.max_sql_executions:
            return f"已达到最大 SQL 执行次数({self.agent_config.max_sql_executions})，请用已有数据完成分析。"

        result = self.sql_tool.execute_query(sql)
        self._sql_count += 1

        if not result["success"]:
            return f"SQL 执行失败: {result['error']}\n请检查 SQL 语法和表名后重试。"

        # 保存 DataFrame，供后续 pandas/chart 引用
        df_name = f"df_{len(self._dataframes)}"
        self._dataframes[df_name] = result["data"]

        # 返回摘要给 LLM
        df = result["data"]
        summary = f"查询成功 ({result['row_count']} 行, {result['elapsed_ms']}ms)，结果已保存为 {df_name}\n"

        if len(df) == 0:
            summary += "查询结果为空。"
        elif len(df) <= 30:
            summary += f"数据:\n{df.to_string(index=False)}"
        else:
            summary += f"前20行:\n{df.head(20).to_string(index=False)}\n...(共{len(df)}行)"

        return summary

    def _handle_pandas(self, args: dict) -> str:
        code = args["code"]
        result = self.pandas_tool.execute_code(code, self._dataframes)

        if not result["success"]:
            return f"代码执行失败: {result['error']}\n请检查代码后重试。"

        # 如果产生了新的 DataFrame，保存
        if isinstance(result["result"], pd.DataFrame):
            df_name = f"df_{len(self._dataframes)}"
            self._dataframes[df_name] = result["result"]
            return f"执行成功，结果已保存为 {df_name}\n{result['result_summary']}"

        output = ""
        if result["output"]:
            output += f"输出:\n{result['output']}\n"
        if result["result_summary"]:
            output += f"返回值:\n{result['result_summary']}"
        return output or "执行成功（无输出）"

    def _handle_chart(self, args: dict) -> str:
        data_ref = args["data_ref"]
        chart_type = args["chart_type"]
        config = args["config"]

        df = self._dataframes.get(data_ref)
        if df is None:
            return f"找不到数据 {data_ref}，可用的有: {list(self._dataframes.keys())}"

        result = self.chart_tool.generate_chart(chart_type, df, config)

        if not result["success"]:
            return f"图表生成失败: {result['error']}"

        self._charts.append(result["path"])
        return f"图表已生成: {result['path']}"

    def _handle_lookup_metric(self, args: dict) -> str:
        keyword = args["keyword"]
        m = self.kb.find_metric(keyword)

        if m:
            return (
                f"指标: {m['name']} (id={m['id']})\n"
                f"描述: {m['description']}\n"
                f"SQL 模板:\n{m.get('sql_template', '无')}"
            )

        return f"未找到与 '{keyword}' 相关的指标定义，请直接根据表结构自行编写 SQL。"

    def _handle_finish(self, args: dict) -> str:
        return "分析完成。"
