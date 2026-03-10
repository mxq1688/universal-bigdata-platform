"""
业务知识库 - 加载和查询业务指标/表结构/术语映射
"""
import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


class KnowledgeBase:
    """业务指标知识库"""

    def __init__(self, config_path: str | Path = None):
        if config_path is None:
            config_path = Path(__file__).resolve().parent.parent.parent / "config" / "knowledge.yaml"

        self.config_path = Path(config_path)
        self.data = self._load()

        self.tables = self.data.get("database", {}).get("tables", [])
        self.metrics = self.data.get("metrics", [])
        self.term_mappings = self.data.get("term_mappings", {})

        logger.info(
            f"知识库已加载: {len(self.tables)} 张表, "
            f"{len(self.metrics)} 个指标, "
            f"{len(self.term_mappings)} 个术语映射"
        )

    def _load(self) -> dict:
        if not self.config_path.exists():
            logger.warning(f"知识库文件不存在: {self.config_path}")
            return {}
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    # ======================== 查询 ========================

    def find_metric(self, keyword: str) -> dict | None:
        """根据关键词查找指标"""
        # 先查术语映射
        metric_id = self.term_mappings.get(keyword, keyword)

        for m in self.metrics:
            if m["id"] == metric_id:
                return m
            if keyword in m.get("name", "") or keyword in m.get("description", ""):
                return m
        return None

    def find_table(self, keyword: str) -> dict | None:
        """根据关键词查找表"""
        for t in self.tables:
            if keyword in t["name"] or keyword in t.get("description", ""):
                return t
        return None

    def get_table_schema_text(self) -> str:
        """生成表结构的文本描述（用于 prompt）"""
        lines = []
        for table in self.tables:
            schema = table.get("schema", "")
            full_name = f"{schema}.{table['name']}" if schema else table["name"]
            lines.append(f"### {full_name}")
            lines.append(f"{table.get('description', '')}")
            lines.append("| 字段 | 类型 | 描述 |")
            lines.append("|------|------|------|")
            for col in table.get("columns", []):
                lines.append(f"| {col['name']} | {col['type']} | {col['desc']} |")
            lines.append("")
        return "\n".join(lines)

    def get_metrics_text(self) -> str:
        """生成指标定义的文本描述（用于 prompt）"""
        lines = []
        for m in self.metrics:
            lines.append(f"- **{m['name']}** (`{m['id']}`): {m['description']}")
        return "\n".join(lines)

    def get_all_table_names(self) -> list[str]:
        """获取所有表名"""
        result = []
        for t in self.tables:
            schema = t.get("schema", "")
            full_name = f"{schema}.{t['name']}" if schema else t["name"]
            result.append(full_name)
        return result

    def suggest_tables(self, query: str) -> list[dict]:
        """根据用户问题推荐可能用到的表"""
        keywords = set(query.lower().split())
        scored = []
        for table in self.tables:
            score = 0
            text = f"{table['name']} {table.get('description', '')}".lower()
            for col in table.get("columns", []):
                text += f" {col['name']} {col['desc']}".lower()
            for kw in keywords:
                if kw in text:
                    score += 1
            if score > 0:
                scored.append((score, table))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [t for _, t in scored]
