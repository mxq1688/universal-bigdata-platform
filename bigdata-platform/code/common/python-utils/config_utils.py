"""
配置加载工具类
支持 .env / YAML / JSON 配置文件
"""
import os
import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ConfigLoader:
    """通用配置加载器"""

    def __init__(self):
        self._config = {}

    def load_env(self, env_file=".env"):
        """加载 .env 文件"""
        env_path = Path(env_file)
        if not env_path.exists():
            logger.warning(".env 文件不存在: %s", env_file)
            return self

        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                self._config[key] = value
                os.environ.setdefault(key, value)

        logger.info("加载 .env: %s (%s 项)", env_file, len(self._config))
        return self

    def load_yaml(self, yaml_file):
        """加载 YAML 文件"""
        import yaml
        yaml_path = Path(yaml_file)
        if not yaml_path.exists():
            logger.warning("YAML 文件不存在: %s", yaml_file)
            return self

        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            self._config.update(self._flatten_dict(data))

        logger.info("加载 YAML: %s", yaml_file)
        return self

    def load_json(self, json_file):
        """加载 JSON 文件"""
        import json
        json_path = Path(json_file)
        if not json_path.exists():
            logger.warning("JSON 文件不存在: %s", json_file)
            return self

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            self._config.update(self._flatten_dict(data))

        logger.info("加载 JSON: %s", json_file)
        return self

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值 (优先级: 环境变量 > 配置文件)"""
        return os.getenv(key, self._config.get(key, default))

    def get_int(self, key: str, default: int = 0) -> int:
        """获取整数配置"""
        value = self.get(key)
        try:
            return int(value) if value is not None else default
        except (ValueError, TypeError):
            return default

    def get_float(self, key: str, default: float = 0.0) -> float:
        """获取浮点数配置"""
        value = self.get(key)
        try:
            return float(value) if value is not None else default
        except (ValueError, TypeError):
            return default

    def get_bool(self, key: str, default: bool = False) -> bool:
        """获取布尔配置"""
        value = self.get(key)
        if value is None:
            return default
        return str(value).lower() in ("true", "1", "yes", "on")

    def get_list(self, key: str, sep: str = ",", default: Optional[list] = None) -> list:
        """获取列表配置"""
        value = self.get(key)
        if value is None:
            return default or []
        return [item.strip() for item in str(value).split(sep) if item.strip()]

    def require(self, key: str) -> str:
        """获取必需配置（不存在则报错）"""
        value = self.get(key)
        if value is None:
            raise ValueError(f"缺少必需配置: {key}")
        return value

    @property
    def all(self) -> dict:
        """获取所有配置"""
        return dict(self._config)

    @staticmethod
    def _flatten_dict(d, parent_key="", sep="_"):
        """扁平化嵌套字典"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(ConfigLoader._flatten_dict(v, new_key, sep).items())
            else:
                items.append((new_key.upper(), str(v)))
        return dict(items)
