"""
Data Analysis Agent - 单元测试
覆盖: 配置加载 / SQL 安全 / Pandas 沙箱 / 图表生成 / 知识库
"""
import os
import sys
import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime

import pandas as pd
import numpy as np

# 项目路径
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ====================== 配置测试 ======================

class TestConfig:
    """配置加载测试"""

    def test_load_config_defaults(self):
        from agent import load_config
        llm, db, agent = load_config()
        assert llm.provider in ("openai", "anthropic")
        assert db.db_type in ("mysql", "postgresql", "clickhouse")
        assert agent.max_sql_executions > 0
        assert agent.max_query_rows > 0

    def test_db_uri_mysql(self):
        from agent import DBConfig
        db = DBConfig(db_type="mysql", host="localhost", port=3306,
                      user="root", password="pass", database="test")
        assert db.uri == "mysql+pymysql://root:pass@localhost:3306/test"

    def test_db_uri_postgresql(self):
        from agent import DBConfig
        db = DBConfig(db_type="postgresql", host="pg-host", port=5432,
                      user="pguser", password="pgpass", database="mydb")
        assert db.uri == "postgresql+psycopg2://pguser:pgpass@pg-host:5432/mydb"

    def test_llm_config_from_env(self):
        from agent import LLMConfig
        cfg = LLMConfig(provider="openai", api_key="sk-test", model="gpt-4o")
        assert cfg.api_key == "sk-test"
        assert cfg.model == "gpt-4o"
        assert cfg.temperature == 0.1


# ====================== SQL 安全测试 ======================

class TestSQLSafety:
    """SQL 安全检查测试"""

    def setup_method(self):
        from agent import DBConfig, AgentConfig
        self.db_config = DBConfig(host="localhost", database="test")
        self.agent_config = AgentConfig()

    def test_select_allowed(self):
        from agent.tools.sql_tool import SQLTool
        tool = SQLTool.__new__(SQLTool)
        tool.max_rows = 5000
        assert tool._safety_check("SELECT * FROM orders") is None

    def test_show_allowed(self):
        from agent.tools.sql_tool import SQLTool
        tool = SQLTool.__new__(SQLTool)
        assert tool._safety_check("SHOW TABLES") is None

    def test_with_cte_allowed(self):
        from agent.tools.sql_tool import SQLTool
        tool = SQLTool.__new__(SQLTool)
        assert tool._safety_check("WITH t AS (SELECT 1) SELECT * FROM t") is None

    def test_insert_blocked(self):
        from agent.tools.sql_tool import SQLTool
        tool = SQLTool.__new__(SQLTool)
        result = tool._safety_check("INSERT INTO orders VALUES (1)")
        assert result is not None
        assert "安全拦截" in result

    def test_drop_blocked(self):
        from agent.tools.sql_tool import SQLTool
        tool = SQLTool.__new__(SQLTool)
        result = tool._safety_check("DROP TABLE orders")
        assert result is not None

    def test_delete_blocked(self):
        from agent.tools.sql_tool import SQLTool
        tool = SQLTool.__new__(SQLTool)
        result = tool._safety_check("DELETE FROM orders WHERE id = 1")
        assert result is not None

    def test_update_blocked(self):
        from agent.tools.sql_tool import SQLTool
        tool = SQLTool.__new__(SQLTool)
        result = tool._safety_check("UPDATE orders SET status = 'done'")
        assert result is not None

    def test_hidden_injection_blocked(self):
        from agent.tools.sql_tool import SQLTool
        tool = SQLTool.__new__(SQLTool)
        # SELECT 开头但内含 DROP
        result = tool._safety_check("SELECT 1; DROP TABLE orders")
        assert result is not None

    def test_add_limit(self):
        from agent.tools.sql_tool import SQLTool
        tool = SQLTool.__new__(SQLTool)
        tool.max_rows = 1000
        result = tool._add_limit("SELECT * FROM orders")
        assert "LIMIT 1000" in result

    def test_no_double_limit(self):
        from agent.tools.sql_tool import SQLTool
        tool = SQLTool.__new__(SQLTool)
        tool.max_rows = 1000
        result = tool._add_limit("SELECT * FROM orders LIMIT 10")
        assert result.count("LIMIT") == 1


# ====================== Pandas 沙箱测试 ======================

class TestPandasTool:
    """Pandas 沙箱测试"""

    def setup_method(self):
        from agent.tools.pandas_tool import PandasTool
        self.tool = PandasTool()
        self.sample_df = pd.DataFrame({
            'city': ['北京', '上海', '广州', '深圳', '杭州'],
            'gmv': [100000, 85000, 70000, 65000, 55000],
            'orders': [500, 420, 350, 310, 270],
        })

    def test_basic_calculation(self):
        result = self.tool.execute_code("1 + 1", {})
        assert result["success"] is True
        assert result["result"] == 2

    def test_dataframe_access(self):
        result = self.tool.execute_code(
            "df_0['gmv'].sum()",
            {"df_0": self.sample_df}
        )
        assert result["success"] is True
        assert result["result"] == 375000

    def test_pandas_operations(self):
        result = self.tool.execute_code(
            "df_0.groupby('city')['gmv'].sum().reset_index()",
            {"df_0": self.sample_df}
        )
        assert result["success"] is True
        assert isinstance(result["result"], pd.DataFrame)

    def test_numpy_available(self):
        result = self.tool.execute_code("np.mean([1, 2, 3, 4, 5])", {})
        assert result["success"] is True
        assert result["result"] == 3.0

    def test_print_captured(self):
        result = self.tool.execute_code(
            "print('hello world')\n42",
            {}
        )
        assert result["success"] is True
        assert "hello world" in result["output"]

    def test_syntax_error_caught(self):
        result = self.tool.execute_code("def incomplete(:", {})
        assert result["success"] is False
        assert result["error"] is not None

    def test_runtime_error_caught(self):
        result = self.tool.execute_code("1 / 0", {})
        assert result["success"] is False
        assert "ZeroDivision" in result["error"]

    def test_describe_dataframe(self):
        summary = self.tool.describe_dataframe(self.sample_df)
        assert "5 行" in summary
        assert "3 列" in summary
        assert "gmv" in summary

    def test_group_analysis(self):
        df = pd.DataFrame({
            'category': ['A', 'A', 'B', 'B'],
            'amount': [100, 200, 300, 400],
        })
        result = self.tool.group_analysis(df, 'category', 'amount', 'sum')
        assert len(result) == 2
        assert result[result['category'] == 'A']['amount'].values[0] == 300


# ====================== 图表生成测试 ======================

class TestChartTool:
    """图表生成测试"""

    def setup_method(self):
        from agent.tools.chart_tool import ChartTool
        self.tmp_dir = tempfile.mkdtemp()
        self.tool = ChartTool(self.tmp_dir)
        self.sample_df = pd.DataFrame({
            'date': ['03-01', '03-02', '03-03', '03-04', '03-05'],
            'gmv': [10000, 12000, 11000, 15000, 13000],
            'orders': [100, 120, 110, 150, 130],
        })

    def test_line_chart(self):
        result = self.tool.generate_chart(
            'line', self.sample_df,
            {'title': '测试折线图', 'x': 'date', 'y': 'gmv'}
        )
        assert result["success"] is True
        assert Path(result["path"]).exists()

    def test_bar_chart(self):
        result = self.tool.generate_chart(
            'bar', self.sample_df,
            {'title': '测试柱状图', 'x': 'date', 'y': 'gmv'}
        )
        assert result["success"] is True

    def test_pie_chart(self):
        df = pd.DataFrame({
            'category': ['数码', '服装', '食品', '家居'],
            'amount': [3000, 2500, 2000, 1500],
        })
        result = self.tool.generate_chart(
            'pie', df,
            {'title': '测试饼图', 'x': 'category', 'y': 'amount'}
        )
        assert result["success"] is True

    def test_multi_y_line(self):
        result = self.tool.generate_chart(
            'line', self.sample_df,
            {'title': '双Y轴折线图', 'x': 'date', 'y': ['gmv', 'orders']}
        )
        assert result["success"] is True

    def test_area_chart(self):
        result = self.tool.generate_chart(
            'area', self.sample_df,
            {'title': '面积图', 'x': 'date', 'y': 'gmv'}
        )
        assert result["success"] is True

    def test_invalid_chart_type(self):
        result = self.tool.generate_chart(
            'invalid_type', self.sample_df,
            {'title': 'test', 'x': 'date', 'y': 'gmv'}
        )
        assert result["success"] is False

    def test_top_n(self):
        result = self.tool.generate_chart(
            'bar', self.sample_df,
            {'title': 'Top 3', 'x': 'date', 'y': 'gmv', 'sort_by': 'gmv', 'top_n': 3}
        )
        assert result["success"] is True

    def test_filename_generation(self):
        name = self.tool._gen_filename("测试图表")
        assert name.startswith("chart_")
        assert name.endswith(".png")


# ====================== 知识库测试 ======================

class TestKnowledgeBase:
    """知识库测试"""

    def setup_method(self):
        from agent.knowledge.base import KnowledgeBase
        self.kb = KnowledgeBase()

    def test_knowledge_loaded(self):
        assert self.kb.data is not None
        assert len(self.kb.tables) > 0

    def test_find_metric_by_id(self):
        m = self.kb.find_metric("gmv")
        assert m is not None
        assert m["id"] == "gmv"

    def test_find_metric_by_name(self):
        m = self.kb.find_metric("日交易额")
        assert m is not None

    def test_find_metric_via_term_mapping(self):
        m = self.kb.find_metric("GMV")
        assert m is not None
        assert m["id"] == "gmv"

    def test_find_metric_not_found(self):
        m = self.kb.find_metric("不存在的指标_xyz")
        assert m is None

    def test_get_table_schema_text(self):
        text = self.kb.get_table_schema_text()
        assert "dwd_trade_order_detail" in text
        assert "order_id" in text

    def test_get_metrics_text(self):
        text = self.kb.get_metrics_text()
        assert "GMV" in text or "gmv" in text

    def test_get_all_table_names(self):
        names = self.kb.get_all_table_names()
        assert len(names) > 0
        assert any("dwd" in n for n in names)

    def test_suggest_tables_for_order_query(self):
        tables = self.kb.suggest_tables("订单 GMV 交易额")
        assert len(tables) > 0


# ====================== Tool Definitions 测试 ======================

class TestToolDefinitions:
    """工具定义完整性测试"""

    def test_all_tools_defined(self):
        from agent.tools import TOOL_DEFINITIONS
        tool_names = {t["name"] for t in TOOL_DEFINITIONS}
        expected = {"execute_sql", "run_pandas_code", "generate_chart",
                    "lookup_metric", "finish_analysis"}
        assert tool_names == expected

    def test_tool_has_required_fields(self):
        from agent.tools import TOOL_DEFINITIONS
        for tool in TOOL_DEFINITIONS:
            assert "name" in tool
            assert "description" in tool
            assert "parameters" in tool
            assert tool["parameters"]["type"] == "object"
            assert "properties" in tool["parameters"]

    def test_tool_has_required_params(self):
        from agent.tools import TOOL_DEFINITIONS
        for tool in TOOL_DEFINITIONS:
            required = tool["parameters"].get("required", [])
            properties = tool["parameters"]["properties"]
            for r in required:
                assert r in properties, f"Tool {tool['name']}: required param '{r}' not in properties"


# ====================== System Prompt 测试 ======================

class TestSystemPrompt:
    """System Prompt 构建测试"""

    def test_build_prompt(self):
        from agent.prompts.system import build_system_prompt
        knowledge = {
            "database": {"tables": [{
                "name": "test_table", "schema": "dwd",
                "description": "测试表",
                "columns": [{"name": "id", "type": "int", "desc": "主键"}]
            }]},
            "metrics": [{"id": "test_metric", "name": "测试指标", "description": "测试用"}]
        }
        prompt = build_system_prompt(knowledge, max_sql_executions=5)
        assert "test_table" in prompt
        assert "测试指标" in prompt
        assert "5" in prompt

    def test_empty_knowledge(self):
        from agent.prompts.system import build_system_prompt
        prompt = build_system_prompt({}, max_sql_executions=10)
        assert "(未配置)" in prompt


# ====================== 集成测试 (Mock LLM) ======================

class TestAgentIntegration:
    """Agent 集成测试 (Mock LLM 调用)"""

    def test_dispatch_unknown_tool(self):
        from agent.core import DataAnalysisAgent
        agent = DataAnalysisAgent.__new__(DataAnalysisAgent)
        agent._dataframes = {}
        agent._charts = []
        agent._steps = []
        agent._sql_count = 0
        result = agent._dispatch_tool("unknown_tool", {})
        assert "未知工具" in result

    def test_handle_finish(self):
        from agent.core import DataAnalysisAgent
        agent = DataAnalysisAgent.__new__(DataAnalysisAgent)
        result = agent._handle_finish({"summary": "测试结论"})
        assert "分析完成" in result

    def test_handle_lookup_metric_found(self):
        from agent.core import DataAnalysisAgent
        from agent.knowledge.base import KnowledgeBase
        agent = DataAnalysisAgent.__new__(DataAnalysisAgent)
        agent.kb = KnowledgeBase()
        result = agent._handle_lookup_metric({"keyword": "GMV"})
        assert "gmv" in result.lower() or "交易额" in result

    def test_handle_lookup_metric_not_found(self):
        from agent.core import DataAnalysisAgent
        from agent.knowledge.base import KnowledgeBase
        agent = DataAnalysisAgent.__new__(DataAnalysisAgent)
        agent.kb = KnowledgeBase()
        result = agent._handle_lookup_metric({"keyword": "不存在的指标xyz"})
        assert "未找到" in result

    def test_analysis_result_dataclass(self):
        from agent.core import AnalysisResult
        r = AnalysisResult(query="测试")
        assert r.query == "测试"
        assert r.success is True
        assert r.charts == []
        assert r.steps == []
        assert r.sql_count == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
