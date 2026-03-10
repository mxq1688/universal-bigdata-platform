# 🤖 Data Analysis Agent

> LLM + ReAct 架构的智能数据分析 Agent。输入自然语言问题，自动完成 SQL 查询 → 数据分析 → 图表生成 → 业务结论。

## 架构

```
用户问题 ("昨天GMV多少？")
    │
    ▼
┌──────────────┐
│   Agent Core │  ReAct 循环 (推理→工具→观察→推理→...)
│   (core.py)  │
└──────┬───────┘
       │
  ┌────┴─────┬──────────┬──────────┬──────────┐
  ▼          ▼          ▼          ▼          ▼
SQLTool   PandasTool  ChartTool  KnowledgeBase  LLM
(查数据)  (二次分析)  (画图表)   (业务术语)    (GPT-4o)
  │          │          │
  ▼          ▼          ▼
 Doris    DataFrame   PNG 图表
```

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置环境变量
cp .env.example .env
# 编辑 .env 填入 OPENAI_API_KEY 和数据库连接

# 3. 交互模式
python main.py

# 4. 单次查询
python main.py -q "过去7天GMV趋势"

# 5. Web 服务
python main.py --web
```

## 运行测试

```bash
pytest tests/ -v
```

## 项目结构

```
agent-service/
├── main.py                    # CLI 入口 (交互/查询/Web)
├── agent/
│   ├── __init__.py            # 配置管理 (LLMConfig, DBConfig, AgentConfig)
│   ├── core.py                # Agent 核心引擎 (ReAct 循环)
│   ├── llm.py                 # LLM 调用层 (OpenAI / Anthropic)
│   ├── tools/
│   │   ├── __init__.py        # 工具定义 (Function Calling schema)
│   │   ├── sql_tool.py        # SQL 查询 (安全检查 + LIMIT)
│   │   ├── pandas_tool.py     # Pandas 沙箱 (二次分析)
│   │   └── chart_tool.py      # 图表生成 (7 种图表类型)
│   ├── knowledge/
│   │   └── base.py            # 知识库 (表结构 + 指标 + 术语)
│   └── prompts/
│       └── system.py          # System Prompt 模板
├── config/
│   └── knowledge.yaml         # 业务指标知识库
├── web/
│   ├── app.py                 # FastAPI Web 服务
│   ├── templates/index.html   # Web UI
│   └── static/                # CSS + JS
├── tests/
│   └── test_agent.py          # 单元测试 (40+ 测试用例)
├── Dockerfile                 # Docker 镜像
└── requirements.txt           # Python 依赖
```

## API

### POST /api/analyze

```json
// 请求
{"query": "昨天的 GMV 和订单量是多少？"}

// 响应
{
  "success": true,
  "query": "昨天的 GMV 和订单量是多少？",
  "summary": "## 📊 昨日经营概览\n\n### 🔍 核心发现\n- GMV: 95,234.00 元\n- 订单量: 487 笔...",
  "charts": ["output/charts/chart_20260309_abc123.png"],
  "sql_count": 2,
  "step_count": 5,
  "elapsed_ms": 3420.5
}
```

### GET /health

```json
{"status": "ok", "timestamp": "2026-03-09T15:30:00", "version": "1.0.0"}
```
