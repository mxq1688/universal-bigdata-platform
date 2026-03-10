"""
Data Analysis Agent - Web API (FastAPI)
提供 RESTful API 和简易 Web UI
"""
import sys
import logging
from pathlib import Path
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agent.core import DataAnalysisAgent, AnalysisResult
from agent.metrics import create_metrics_endpoint, ANALYSIS_REQUESTS, ANALYSIS_DURATION, ACTIVE_ANALYSES, INFO

# 日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
for name in ("httpx", "httpcore", "openai", "anthropic", "urllib3", "sqlalchemy"):
    logging.getLogger(name).setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# FastAPI
app = FastAPI(title="Data Analysis Agent", version="1.0.0", description="自然语言数据分析 Agent API")

# 静态资源 & 模板
STATIC_DIR = Path(__file__).parent / "static"
TEMPLATE_DIR = Path(__file__).parent / "templates"
STATIC_DIR.mkdir(parents=True, exist_ok=True)
TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))

# Prometheus 指标端点
metrics_router = create_metrics_endpoint()
app.include_router(metrics_router)

# Agent 单例
_agent: DataAnalysisAgent | None = None


def get_agent() -> DataAnalysisAgent:
    global _agent
    if _agent is None:
        logger.info("初始化 Agent...")
        _agent = DataAnalysisAgent()
        logger.info("Agent 初始化完成")
    return _agent


# ======================== 数据模型 ========================

class AnalyzeRequest(BaseModel):
    query: str
    """自然语言分析问题"""


class AnalyzeResponse(BaseModel):
    success: bool
    query: str
    summary: str
    charts: list[str]
    sql_count: int
    step_count: int
    elapsed_ms: float
    error: str | None = None
    steps: list[dict] = []


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    version: str


# ======================== API 路由 ========================

@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="ok",
        timestamp=datetime.now().isoformat(),
        version="1.0.0",
    )


@app.post("/api/analyze", response_model=AnalyzeResponse)
async def analyze(req: AnalyzeRequest):
    """
    核心分析接口
    POST /api/analyze
    Body: {"query": "昨天的 GMV 是多少？"}
    """
    import time as _time

    if not req.query.strip():
        raise HTTPException(status_code=400, detail="query 不能为空")

    logger.info(f"收到分析请求: {req.query}")
    ACTIVE_ANALYSES.inc()
    start = _time.time()

    try:
        agent = get_agent()
        result = agent.analyze(req.query)
        status = "success" if result.success else "error"
        ANALYSIS_REQUESTS.inc(status=status)
    except Exception as e:
        ANALYSIS_REQUESTS.inc(status="error")
        raise
    finally:
        ANALYSIS_DURATION.observe(_time.time() - start)
        ACTIVE_ANALYSES.dec()

    return AnalyzeResponse(
        success=result.success,
        query=result.query,
        summary=result.summary,
        charts=result.charts,
        sql_count=result.sql_count,
        step_count=len(result.steps),
        elapsed_ms=result.elapsed_ms,
        error=result.error,
        steps=result.steps,
    )


@app.get("/api/charts/{filename}")
async def get_chart(filename: str):
    """获取生成的图表文件"""
    agent = get_agent()
    chart_dir = Path(agent.agent_config.chart_output_dir)
    file_path = chart_dir / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="图表不存在")
    return FileResponse(str(file_path), media_type="image/png")


# ======================== Web UI ========================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ======================== 启动 ========================

def start_server(host: str = "0.0.0.0", port: int = 8501):
    import uvicorn
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    import os
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", "8501"))
    start_server(host, port)
