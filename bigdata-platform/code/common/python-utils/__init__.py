"""
大数据平台 Python 工具类库

用法:
    # 方式 1: 直接 import 单个模块
    from db_utils import DBUtils
    from hdfs_utils import HDFSUtils

    # 方式 2: 作为包 import (需将 python-utils 加入 PYTHONPATH)
    from common_utils import DBUtils, HDFSUtils, KafkaUtils, ConfigLoader
"""

__version__ = "1.0.0"

# 延迟导入，避免缺少依赖时直接报错
def _lazy_import():
    try:
        from .db_utils import DBUtils
        from .hdfs_utils import HDFSUtils
        from .kafka_utils import KafkaUtils
        from .config_utils import ConfigLoader
        return DBUtils, HDFSUtils, KafkaUtils, ConfigLoader
    except ImportError:
        return None, None, None, None

__all__ = ["DBUtils", "HDFSUtils", "KafkaUtils", "ConfigLoader"]
