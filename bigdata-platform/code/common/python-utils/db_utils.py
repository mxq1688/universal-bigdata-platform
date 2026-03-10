"""
数据库连接工具类 (Python 版)
支持 MySQL / PostgreSQL / Doris，基于 SQLAlchemy + 连接池
"""
import os
import logging
from contextlib import contextmanager
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)


class DBUtils:
    """数据库连接池工具"""

    def __init__(self, db_type="mysql", host="localhost", port=3306,
                 database="bigdata", username="root", password="",
                 pool_size=5, max_overflow=10, pool_timeout=30):
        self.db_type = db_type
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

        self.engine = self._create_engine(pool_size, max_overflow, pool_timeout)
        logger.info("数据库连接池初始化完成: %s://%s:%s/%s", db_type, host, port, database)

    def _create_engine(self, pool_size, max_overflow, pool_timeout):
        """创建 SQLAlchemy Engine"""
        driver_map = {
            "mysql": "mysql+pymysql",
            "postgresql": "postgresql+psycopg2",
            "doris": "mysql+pymysql",
        }
        driver = driver_map.get(self.db_type, "mysql+pymysql")
        encoded_password = quote_plus(self.password)
        url = f"{driver}://{self.username}:{encoded_password}@{self.host}:{self.port}/{self.database}"

        extra_args = {}
        if self.db_type == "mysql" or self.db_type == "doris":
            url += "?charset=utf8mb4"

        return create_engine(
            url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_pre_ping=True,
            pool_recycle=3600,
            **extra_args,
        )

    @contextmanager
    def get_connection(self):
        """获取连接（上下文管理器，自动归还）"""
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, sql, params=None):
        """执行 SQL（INSERT/UPDATE/DELETE）"""
        with self.get_connection() as conn:
            result = conn.execute(text(sql), params or {})
            conn.commit()
            logger.debug("SQL 执行成功: %s 行受影响", result.rowcount)
            return result.rowcount

    def query(self, sql, params=None):
        """执行查询，返回 list[dict]"""
        with self.get_connection() as conn:
            result = conn.execute(text(sql), params or {})
            columns = list(result.keys())
            rows = [dict(zip(columns, row)) for row in result.fetchall()]
            logger.debug("查询返回 %s 条数据", len(rows))
            return rows

    def query_one(self, sql, params=None):
        """查询单行"""
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def query_scalar(self, sql, params=None):
        """查询单值"""
        with self.get_connection() as conn:
            result = conn.execute(text(sql), params or {})
            row = result.fetchone()
            return row[0] if row else None

    def batch_execute(self, sql, params_list):
        """批量执行"""
        with self.get_connection() as conn:
            for params in params_list:
                conn.execute(text(sql), params)
            conn.commit()
            logger.debug("批量执行完成: %s 条", len(params_list))

    def close(self):
        """关闭连接池"""
        if self.engine:
            self.engine.dispose()
            logger.info("数据库连接池已关闭")

    @classmethod
    def from_env(cls):
        """从环境变量创建"""
        return cls(
            db_type=os.getenv("DB_TYPE", "mysql"),
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "3306")),
            database=os.getenv("DB_NAME", "bigdata"),
            username=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", ""),
        )
