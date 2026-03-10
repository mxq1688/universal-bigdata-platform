"""
工具注册表 - 将所有工具暴露为 Function Calling 格式
"""

TOOL_DEFINITIONS = [
    {
        "name": "execute_sql",
        "description": "执行 SQL 查询语句，从数据库中获取数据。只允许 SELECT 查询，禁止写操作。返回 DataFrame 格式的查询结果。",
        "parameters": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "要执行的 SQL 查询语句。必须是 SELECT 语句。示例: SELECT COUNT(*) FROM dwd.dwd_trade_order_detail WHERE dt = '2026-03-08'",
                },
                "purpose": {
                    "type": "string",
                    "description": "这条 SQL 的分析目的，用于日志追踪。示例: '查询昨日订单总量'",
                },
            },
            "required": ["sql"],
        },
    },
    {
        "name": "run_pandas_code",
        "description": "执行 Pandas/NumPy 数据分析代码。可以对之前 SQL 查询的结果进行二次加工、统计分析、特征计算等。可用的 DataFrame 变量名和之前 execute_sql 返回的 df_名 一致。",
        "parameters": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "Pandas 分析代码。可用模块: pd(pandas), np(numpy)。可用变量: 之前查询产生的 df_0, df_1 等 DataFrame。最后一行表达式的值会作为结果返回。",
                },
                "purpose": {
                    "type": "string",
                    "description": "这段代码的分析目的",
                },
            },
            "required": ["code"],
        },
    },
    {
        "name": "generate_chart",
        "description": "根据数据生成可视化图表。支持折线图、柱状图、饼图、散点图、热力图、漏斗图、面积图。",
        "parameters": {
            "type": "object",
            "properties": {
                "chart_type": {
                    "type": "string",
                    "enum": ["line", "bar", "pie", "scatter", "heatmap", "funnel", "area"],
                    "description": "图表类型",
                },
                "data_ref": {
                    "type": "string",
                    "description": "引用之前查询结果的变量名，如 df_0, df_1",
                },
                "config": {
                    "type": "object",
                    "description": "图表配置",
                    "properties": {
                        "title": {"type": "string", "description": "图表标题"},
                        "x": {"type": "string", "description": "x 轴字段名"},
                        "y": {
                            "description": "y 轴字段名，可以是字符串或字符串数组",
                        },
                        "sort_by": {"type": "string", "description": "排序字段（可选）"},
                        "top_n": {"type": "integer", "description": "只展示 top N（可选）"},
                    },
                    "required": ["title", "x", "y"],
                },
            },
            "required": ["chart_type", "data_ref", "config"],
        },
    },
    {
        "name": "lookup_metric",
        "description": "查找业务指标定义。根据指标名称或 ID，返回指标的定义、SQL 模板和维度信息。用于理解业务术语。",
        "parameters": {
            "type": "object",
            "properties": {
                "keyword": {
                    "type": "string",
                    "description": "要查找的指标名称或关键词，如 '日活', 'GMV', '客单价'",
                },
            },
            "required": ["keyword"],
        },
    },
    {
        "name": "finish_analysis",
        "description": "分析完成，输出最终的分析结论。在所有数据查询和图表生成完成后调用。",
        "parameters": {
            "type": "object",
            "properties": {
                "summary": {
                    "type": "string",
                    "description": "分析结论摘要（纯文本，支持 Markdown）。包含核心发现、数据指标、业务建议。",
                },
                "charts": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "本次分析生成的图表文件路径列表",
                },
            },
            "required": ["summary"],
        },
    },
]
