"""
Data Analysis Agent - 配置管理
"""
import os
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"
OUTPUT_DIR = BASE_DIR / "output"


@dataclass
class LLMConfig:
    provider: str = "openai"  # openai / anthropic
    api_key: str = ""
    model: str = "gpt-4o"
    base_url: str = "https://api.openai.com/v1"
    temperature: float = 0.1
    max_tokens: int = 4096


@dataclass
class DBConfig:
    db_type: str = "mysql"
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "bigdata"

    @property
    def uri(self) -> str:
        drivers = {
            "mysql": "mysql+pymysql",
            "postgresql": "postgresql+psycopg2",
            "clickhouse": "clickhousedb",
        }
        driver = drivers.get(self.db_type, "mysql+pymysql")
        return f"{driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class AgentConfig:
    max_sql_executions: int = 10
    max_query_rows: int = 5000
    chart_output_dir: str = str(OUTPUT_DIR / "charts")
    report_output_dir: str = str(OUTPUT_DIR / "reports")


def load_config():
    """从环境变量加载配置"""

    # LLM
    provider = "anthropic" if os.getenv("ANTHROPIC_API_KEY") else "openai"
    if provider == "anthropic":
        llm = LLMConfig(
            provider="anthropic",
            api_key=os.getenv("ANTHROPIC_API_KEY", ""),
            model=os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
        )
    else:
        llm = LLMConfig(
            provider="openai",
            api_key=os.getenv("OPENAI_API_KEY", ""),
            model=os.getenv("OPENAI_MODEL", "gpt-4o"),
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        )

    # DB
    db = DBConfig(
        db_type=os.getenv("DB_TYPE", "mysql"),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "bigdata"),
    )

    # Agent
    agent = AgentConfig(
        max_sql_executions=int(os.getenv("MAX_SQL_EXECUTIONS", "10")),
        max_query_rows=int(os.getenv("MAX_QUERY_ROWS", "5000")),
        chart_output_dir=os.getenv("CHART_OUTPUT_DIR", str(OUTPUT_DIR / "charts")),
        report_output_dir=os.getenv("REPORT_OUTPUT_DIR", str(OUTPUT_DIR / "reports")),
    )

    return llm, db, agent


def load_knowledge() -> dict:
    """加载业务指标知识库"""
    knowledge_path = CONFIG_DIR / "knowledge.yaml"
    if not knowledge_path.exists():
        return {}
    with open(knowledge_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
