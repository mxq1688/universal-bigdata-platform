"""
SQL 执行工具 - 安全地查询数据库
"""
import logging
import time
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class SQLTool:
    """SQL 查询执行器"""

    def __init__(self, db_config, agent_config):
        self.db_config = db_config
        self.max_rows = agent_config.max_query_rows
        self.engine = create_engine(
            db_config.uri,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            echo=False,
        )

    # ======================== 核心方法 ========================

    def execute_query(self, sql: str) -> dict:
        """
        执行 SELECT 查询，返回结构化结果
        返回: {
            "success": bool,
            "data": DataFrame,
            "row_count": int,
            "columns": list,
            "elapsed_ms": float,
            "sql": str,
            "error": str | None
        }
        """
        # 安全检查
        check = self._safety_check(sql)
        if check:
            return {
                "success": False, "data": pd.DataFrame(),
                "row_count": 0, "columns": [], "elapsed_ms": 0,
                "sql": sql, "error": check,
            }

        # 添加 LIMIT
        sql_limited = self._add_limit(sql)

        start = time.time()
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(sql_limited), conn)

            elapsed = round((time.time() - start) * 1000, 2)
            logger.info(f"SQL 执行成功 [{elapsed}ms] rows={len(df)}: {sql_limited[:120]}")

            return {
                "success": True,
                "data": df,
                "row_count": len(df),
                "columns": list(df.columns),
                "elapsed_ms": elapsed,
                "sql": sql_limited,
                "error": None,
            }

        except Exception as e:
            elapsed = round((time.time() - start) * 1000, 2)
            error_msg = str(e)
            logger.error(f"SQL 执行失败 [{elapsed}ms]: {error_msg}")
            return {
                "success": False, "data": pd.DataFrame(),
                "row_count": 0, "columns": [], "elapsed_ms": elapsed,
                "sql": sql_limited, "error": error_msg,
            }

    def get_table_schema(self, table_name: str) -> dict:
        """获取表结构"""
        sql_map = {
            "mysql": f"DESCRIBE {table_name}",
            "postgresql": f"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{table_name}'",
            "clickhouse": f"DESCRIBE TABLE {table_name}",
        }
        sql = sql_map.get(self.db_config.db_type, f"DESCRIBE {table_name}")
        return self.execute_query(sql)

    def get_sample_data(self, table_name: str, limit: int = 5) -> dict:
        """获取表样例数据"""
        return self.execute_query(f"SELECT * FROM {table_name} LIMIT {limit}")

    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False

    # ======================== 安全 ========================

    def _safety_check(self, sql: str) -> str | None:
        """
        SQL 安全检查 - 只允许 SELECT / SHOW / DESCRIBE / EXPLAIN
        返回 None 表示安全，否则返回错误信息
        """
        normalized = sql.strip().upper()

        allowed_prefixes = ("SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH")
        if not normalized.startswith(allowed_prefixes):
            return f"安全拦截: 只允许查询操作，当前语句以 '{normalized.split()[0]}' 开头"

        dangerous_keywords = [
            "INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ",
            "TRUNCATE ", "CREATE ", "GRANT ", "REVOKE ",
            "INTO OUTFILE", "INTO DUMPFILE", "LOAD DATA",
        ]
        for kw in dangerous_keywords:
            if kw in normalized:
                return f"安全拦截: 检测到危险关键字 '{kw.strip()}'"

        return None

    def _add_limit(self, sql: str) -> str:
        """给没有 LIMIT 的查询自动加上"""
        upper = sql.strip().upper()
        if "LIMIT" not in upper:
            return f"{sql.rstrip(';')} LIMIT {self.max_rows}"
        return sql
