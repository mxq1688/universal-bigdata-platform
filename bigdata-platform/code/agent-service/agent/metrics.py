"""
Agent Service — Prometheus 指标导出

使用方式:
  - FastAPI 挂载: app.mount("/metrics", metrics_app)
  - 或独立 ASGI: uvicorn agent.metrics:metrics_app --port 9090

指标列表:
  agent_analysis_requests_total    — 分析请求总数 (counter)
  agent_analysis_errors_total      — 分析错误总数 (counter)
  agent_analysis_duration_seconds  — 分析耗时分布 (histogram)
  agent_sql_executions_total       — SQL 执行次数 (counter)
  agent_chart_generated_total      — 图表生成次数 (counter)
  agent_active_analyses            — 正在进行的分析 (gauge)
  agent_llm_tokens_total           — LLM Token 消耗 (counter)
  agent_info                       — 服务版本信息 (info/gauge)
"""
import time
import threading
from collections import defaultdict


class _Counter:
    """线程安全计数器"""

    def __init__(self, name: str, help_text: str, labels: list[str] = None):
        self.name = name
        self.help = help_text
        self.labels = labels or []
        self._values: dict[tuple, float] = defaultdict(float)
        self._lock = threading.Lock()

    def inc(self, value: float = 1, **kwargs):
        key = tuple(kwargs.get(l, "") for l in self.labels)
        with self._lock:
            self._values[key] += value

    def collect(self) -> str:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} counter"]
        with self._lock:
            for key, val in self._values.items():
                label_str = self._label_str(key)
                lines.append(f"{self.name}{label_str} {val}")
        if not self._values:
            lines.append(f"{self.name} 0")
        return "\n".join(lines)

    def _label_str(self, key: tuple) -> str:
        if not self.labels:
            return ""
        pairs = [f'{l}="{v}"' for l, v in zip(self.labels, key)]
        return "{" + ",".join(pairs) + "}"


class _Gauge:
    """线程安全仪表盘"""

    def __init__(self, name: str, help_text: str, labels: list[str] = None):
        self.name = name
        self.help = help_text
        self.labels = labels or []
        self._values: dict[tuple, float] = defaultdict(float)
        self._lock = threading.Lock()

    def set(self, value: float, **kwargs):
        key = tuple(kwargs.get(l, "") for l in self.labels)
        with self._lock:
            self._values[key] = value

    def inc(self, value: float = 1, **kwargs):
        key = tuple(kwargs.get(l, "") for l in self.labels)
        with self._lock:
            self._values[key] += value

    def dec(self, value: float = 1, **kwargs):
        key = tuple(kwargs.get(l, "") for l in self.labels)
        with self._lock:
            self._values[key] -= value

    def collect(self) -> str:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} gauge"]
        with self._lock:
            for key, val in self._values.items():
                label_str = ""
                if self.labels:
                    pairs = [f'{l}="{v}"' for l, v in zip(self.labels, key)]
                    label_str = "{" + ",".join(pairs) + "}"
                lines.append(f"{self.name}{label_str} {val}")
        if not self._values:
            lines.append(f"{self.name} 0")
        return "\n".join(lines)


class _Histogram:
    """简化版直方图 (10 个分桶)"""

    DEFAULT_BUCKETS = (0.5, 1, 2, 5, 10, 15, 20, 30, 60, 120, float("inf"))

    def __init__(self, name: str, help_text: str, buckets=None):
        self.name = name
        self.help = help_text
        self.buckets = buckets or self.DEFAULT_BUCKETS
        self._sum = 0.0
        self._count = 0
        self._bucket_counts = [0] * len(self.buckets)
        self._lock = threading.Lock()

    def observe(self, value: float):
        with self._lock:
            self._sum += value
            self._count += 1
            for i, bound in enumerate(self.buckets):
                if value <= bound:
                    self._bucket_counts[i] += 1

    def collect(self) -> str:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} histogram"]
        with self._lock:
            cumulative = 0
            for i, bound in enumerate(self.buckets):
                cumulative += self._bucket_counts[i]
                le = "+Inf" if bound == float("inf") else str(bound)
                lines.append(f'{self.name}_bucket{{le="{le}"}} {cumulative}')
            lines.append(f"{self.name}_sum {self._sum}")
            lines.append(f"{self.name}_count {self._count}")
        return "\n".join(lines)


# ====================== 全局指标实例 ======================

# Counters
ANALYSIS_REQUESTS = _Counter(
    "agent_analysis_requests_total",
    "Total number of analysis requests",
    labels=["status"],
)

ANALYSIS_ERRORS = _Counter(
    "agent_analysis_errors_total",
    "Total number of analysis errors",
    labels=["error_type"],
)

SQL_EXECUTIONS = _Counter(
    "agent_sql_executions_total",
    "Total number of SQL queries executed",
    labels=["status"],
)

CHART_GENERATED = _Counter(
    "agent_chart_generated_total",
    "Total number of charts generated",
    labels=["chart_type"],
)

LLM_TOKENS = _Counter(
    "agent_llm_tokens_total",
    "Total LLM tokens consumed",
    labels=["type"],  # prompt / completion
)

# Gauges
ACTIVE_ANALYSES = _Gauge(
    "agent_active_analyses",
    "Number of analyses currently in progress",
)

INFO = _Gauge(
    "agent_info",
    "Agent service information",
    labels=["version", "llm_provider", "llm_model"],
)

# Histograms
ANALYSIS_DURATION = _Histogram(
    "agent_analysis_duration_seconds",
    "Analysis request duration in seconds",
    buckets=(0.5, 1, 2, 5, 10, 15, 20, 30, 60, 120, float("inf")),
)

SQL_DURATION = _Histogram(
    "agent_sql_duration_seconds",
    "SQL query execution duration in seconds",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, float("inf")),
)


# ====================== 注册表 ======================

ALL_METRICS = [
    ANALYSIS_REQUESTS,
    ANALYSIS_ERRORS,
    SQL_EXECUTIONS,
    CHART_GENERATED,
    LLM_TOKENS,
    ACTIVE_ANALYSES,
    INFO,
    ANALYSIS_DURATION,
    SQL_DURATION,
]


def generate_metrics() -> str:
    """生成 Prometheus 文本格式的指标"""
    sections = [m.collect() for m in ALL_METRICS]
    return "\n\n".join(sections) + "\n"


# ====================== FastAPI 路由 ======================

def create_metrics_endpoint():
    """创建 /metrics 端点，可挂载到 FastAPI app"""
    from fastapi import APIRouter
    from fastapi.responses import PlainTextResponse

    router = APIRouter()

    @router.get("/metrics", response_class=PlainTextResponse)
    async def metrics():
        return generate_metrics()

    return router


# ====================== 便捷装饰器 ======================

def track_analysis(func):
    """装饰器: 自动追踪分析请求的计数和耗时"""
    import functools

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        ACTIVE_ANALYSES.inc()
        start = time.time()
        try:
            result = func(*args, **kwargs)
            status = "success" if getattr(result, "success", True) else "error"
            ANALYSIS_REQUESTS.inc(status=status)
            return result
        except Exception as e:
            ANALYSIS_REQUESTS.inc(status="error")
            ANALYSIS_ERRORS.inc(error_type=type(e).__name__)
            raise
        finally:
            duration = time.time() - start
            ANALYSIS_DURATION.observe(duration)
            ACTIVE_ANALYSES.dec()

    return wrapper


def track_sql(func):
    """装饰器: 自动追踪 SQL 执行计数和耗时"""
    import functools

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            result = func(*args, **kwargs)
            SQL_EXECUTIONS.inc(status="success")
            return result
        except Exception as e:
            SQL_EXECUTIONS.inc(status="error")
            raise
        finally:
            SQL_DURATION.observe(time.time() - start)

    return wrapper
