"""
common/python-utils 单元测试
覆盖: ConfigLoader / DBUtils / HDFSUtils / KafkaUtils
"""
import os
import sys
import json
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ====================== ConfigLoader 测试 ======================

class TestConfigLoader:
    """配置加载器测试"""

    def test_load_env_file(self, tmp_path):
        from config_utils import ConfigLoader

        env_file = tmp_path / ".env"
        env_file.write_text(
            "DB_HOST=localhost\n"
            "DB_PORT=3306\n"
            "# comment line\n"
            "\n"
            'DB_PASSWORD="my_secret"\n'
        )

        loader = ConfigLoader()
        loader.load_env(str(env_file))

        assert loader.get("DB_HOST") == "localhost"
        assert loader.get("DB_PORT") == "3306"
        assert loader.get("DB_PASSWORD") == "my_secret"

    def test_load_env_missing_file(self):
        from config_utils import ConfigLoader
        loader = ConfigLoader()
        loader.load_env("/nonexistent/.env")
        assert loader.get("ANYTHING") is None

    def test_get_with_default(self):
        from config_utils import ConfigLoader
        loader = ConfigLoader()
        assert loader.get("MISSING_KEY", "default_val") == "default_val"

    def test_get_int(self, tmp_path):
        from config_utils import ConfigLoader
        env_file = tmp_path / ".env"
        env_file.write_text("PORT=8080\nINVALID=abc\n")

        loader = ConfigLoader()
        loader.load_env(str(env_file))

        assert loader.get_int("PORT") == 8080
        assert loader.get_int("INVALID", 0) == 0
        assert loader.get_int("MISSING", 3000) == 3000

    def test_get_float(self, tmp_path):
        from config_utils import ConfigLoader
        env_file = tmp_path / ".env"
        env_file.write_text("RATE=0.85\n")

        loader = ConfigLoader()
        loader.load_env(str(env_file))
        assert loader.get_float("RATE") == 0.85
        assert loader.get_float("MISSING", 1.0) == 1.0

    def test_get_bool(self, tmp_path):
        from config_utils import ConfigLoader
        env_file = tmp_path / ".env"
        env_file.write_text("ENABLED=true\nDISABLED=false\nYES=1\nNO=0\n")

        loader = ConfigLoader()
        loader.load_env(str(env_file))

        assert loader.get_bool("ENABLED") is True
        assert loader.get_bool("DISABLED") is False
        assert loader.get_bool("YES") is True
        assert loader.get_bool("NO") is False
        assert loader.get_bool("MISSING", False) is False

    def test_get_list(self, tmp_path):
        from config_utils import ConfigLoader
        env_file = tmp_path / ".env"
        env_file.write_text("HOSTS=host1,host2,host3\n")

        loader = ConfigLoader()
        loader.load_env(str(env_file))

        result = loader.get_list("HOSTS")
        assert result == ["host1", "host2", "host3"]
        assert loader.get_list("MISSING") == []

    def test_require_existing_key(self, tmp_path):
        from config_utils import ConfigLoader
        env_file = tmp_path / ".env"
        env_file.write_text("API_KEY=test123\n")

        loader = ConfigLoader()
        loader.load_env(str(env_file))
        assert loader.require("API_KEY") == "test123"

    def test_require_missing_key_raises(self):
        from config_utils import ConfigLoader
        loader = ConfigLoader()
        with pytest.raises(ValueError, match="缺少必需配置"):
            loader.require("NONEXISTENT_KEY")

    def test_load_json(self, tmp_path):
        from config_utils import ConfigLoader
        json_file = tmp_path / "config.json"
        json_file.write_text(json.dumps({
            "database": {"host": "db-server", "port": 5432},
            "cache": {"ttl": 300},
        }))

        loader = ConfigLoader()
        loader.load_json(str(json_file))
        assert loader.get("DATABASE_HOST") == "db-server"
        assert loader.get("DATABASE_PORT") == "5432"
        assert loader.get("CACHE_TTL") == "300"

    def test_env_priority_over_file(self, tmp_path):
        """环境变量优先级高于配置文件"""
        from config_utils import ConfigLoader
        env_file = tmp_path / ".env"
        env_file.write_text("TEST_PRIORITY=from_file\n")

        os.environ["TEST_PRIORITY"] = "from_env"
        try:
            loader = ConfigLoader()
            loader.load_env(str(env_file))
            assert loader.get("TEST_PRIORITY") == "from_env"
        finally:
            os.environ.pop("TEST_PRIORITY", None)

    def test_all_property(self, tmp_path):
        from config_utils import ConfigLoader
        env_file = tmp_path / ".env"
        env_file.write_text("A=1\nB=2\n")

        loader = ConfigLoader()
        loader.load_env(str(env_file))
        all_config = loader.all
        assert isinstance(all_config, dict)
        assert "A" in all_config

    def test_flatten_dict(self):
        from config_utils import ConfigLoader
        result = ConfigLoader._flatten_dict({"a": {"b": {"c": 1}}, "d": 2})
        assert result == {"A_B_C": "1", "D": "2"}


# ====================== DBUtils 测试 ======================

class TestDBUtils:
    """数据库工具测试 (Mock)"""

    def test_create_mysql_engine(self):
        from db_utils import DBUtils
        with patch("db_utils.create_engine") as mock_engine:
            mock_engine.return_value = MagicMock()
            db = DBUtils(db_type="mysql", host="localhost", port=3306,
                         database="test", username="root", password="pass")
            mock_engine.assert_called_once()
            call_args = mock_engine.call_args
            assert "mysql+pymysql" in call_args[0][0]

    def test_create_postgresql_engine(self):
        from db_utils import DBUtils
        with patch("db_utils.create_engine") as mock_engine:
            mock_engine.return_value = MagicMock()
            db = DBUtils(db_type="postgresql", host="pg-host", port=5432,
                         database="mydb", username="pguser", password="pgpass")
            call_url = mock_engine.call_args[0][0]
            assert "postgresql+psycopg2" in call_url

    def test_create_doris_engine(self):
        from db_utils import DBUtils
        with patch("db_utils.create_engine") as mock_engine:
            mock_engine.return_value = MagicMock()
            db = DBUtils(db_type="doris", host="doris-fe", port=9030,
                         database="dw", username="root", password="")
            call_url = mock_engine.call_args[0][0]
            assert "mysql+pymysql" in call_url
            assert "charset=utf8mb4" in call_url

    def test_query_returns_list_dict(self):
        from db_utils import DBUtils
        with patch("db_utils.create_engine") as mock_engine:
            mock_conn = MagicMock()
            mock_result = MagicMock()
            mock_result.keys.return_value = ["id", "name"]
            mock_result.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
            mock_conn.execute.return_value = mock_result
            mock_engine.return_value.connect.return_value.__enter__ = lambda s: mock_conn
            mock_engine.return_value.connect.return_value.__exit__ = lambda s, *a: None

            db = DBUtils.__new__(DBUtils)
            db.engine = mock_engine.return_value

            with patch.object(db, "get_connection") as mock_ctx:
                mock_ctx.return_value.__enter__ = lambda s: mock_conn
                mock_ctx.return_value.__exit__ = lambda s, *a: None
                rows = db.query("SELECT * FROM users")
                assert len(rows) == 2
                assert rows[0] == {"id": 1, "name": "Alice"}

    def test_query_one_returns_first(self):
        from db_utils import DBUtils
        db = DBUtils.__new__(DBUtils)
        with patch.object(db, "query", return_value=[{"id": 1}]):
            result = db.query_one("SELECT 1")
            assert result == {"id": 1}

    def test_query_one_returns_none_when_empty(self):
        from db_utils import DBUtils
        db = DBUtils.__new__(DBUtils)
        with patch.object(db, "query", return_value=[]):
            result = db.query_one("SELECT 1 WHERE 1=0")
            assert result is None

    def test_from_env(self):
        from db_utils import DBUtils
        with patch("db_utils.create_engine") as mock_engine:
            mock_engine.return_value = MagicMock()
            with patch.dict(os.environ, {
                "DB_TYPE": "mysql",
                "DB_HOST": "env-host",
                "DB_PORT": "3307",
                "DB_NAME": "envdb",
                "DB_USER": "envuser",
                "DB_PASSWORD": "envpass",
            }):
                db = DBUtils.from_env()
                assert db.host == "env-host"
                assert db.port == 3307
                assert db.database == "envdb"

    def test_close(self):
        from db_utils import DBUtils
        db = DBUtils.__new__(DBUtils)
        db.engine = MagicMock()
        db.close()
        db.engine.dispose.assert_called_once()


# ====================== HDFSUtils 测试 ======================

class TestHDFSUtils:
    """HDFS 工具测试 (Mock)"""

    def _create_hdfs_with_mock(self):
        """创建带 Mock client 的 HDFSUtils 实例"""
        import hdfs_utils
        hdfs = hdfs_utils.HDFSUtils.__new__(hdfs_utils.HDFSUtils)
        hdfs.namenode_url = "http://localhost:9870"
        hdfs.user = "hadoop"
        hdfs.mode = "webhdfs"
        hdfs.client = MagicMock()
        return hdfs

    def test_mkdir(self):
        hdfs = self._create_hdfs_with_mock()
        hdfs.mkdir("/data/test")
        hdfs.client.makedirs.assert_called_with("/data/test")

    def test_exists_true(self):
        hdfs = self._create_hdfs_with_mock()
        hdfs.client.status.return_value = {"type": "DIRECTORY"}
        assert hdfs.exists("/data/test") is True

    def test_exists_false(self):
        hdfs = self._create_hdfs_with_mock()
        hdfs.client.status.return_value = None
        assert hdfs.exists("/data/nonexistent") is False

    def test_create_partition(self):
        hdfs = self._create_hdfs_with_mock()
        path = hdfs.create_partition("/warehouse/ods", "2026-03-09")
        assert path == "/warehouse/ods/dt=2026-03-09"
        hdfs.client.makedirs.assert_called_with("/warehouse/ods/dt=2026-03-09")

    def test_partition_exists(self):
        hdfs = self._create_hdfs_with_mock()
        hdfs.client.status.return_value = {"type": "DIRECTORY"}
        assert hdfs.partition_exists("/warehouse/ods", "2026-03-09") is True

    def test_invalid_mode_raises(self):
        import hdfs_utils
        with pytest.raises(ValueError, match="不支持的模式"):
            hdfs_utils.HDFSUtils(mode="invalid")

    def test_get_file_size(self):
        hdfs = self._create_hdfs_with_mock()
        hdfs.client.status.return_value = {"length": 1024}
        assert hdfs.get_file_size("/data/file.txt") == 1024

    def test_list_dir(self):
        hdfs = self._create_hdfs_with_mock()
        hdfs.client.list.return_value = ["file1.txt", "file2.csv"]
        result = hdfs.list_dir("/data")
        assert len(result) == 2

    def test_delete(self):
        hdfs = self._create_hdfs_with_mock()
        hdfs.delete("/data/old", recursive=True)
        hdfs.client.delete.assert_called_with("/data/old", recursive=True)

    def test_write_text(self):
        hdfs = self._create_hdfs_with_mock()
        hdfs.write_text("/data/file.txt", "hello world")
        hdfs.client.write.assert_called_once()

    def test_clean_partition_exists(self):
        hdfs = self._create_hdfs_with_mock()
        hdfs.client.status.return_value = {"type": "DIRECTORY"}
        path = hdfs.clean_partition("/warehouse/ods", "2026-03-09")
        assert path == "/warehouse/ods/dt=2026-03-09"
        hdfs.client.delete.assert_called_once()

    def test_from_env(self):
        import hdfs_utils
        with patch.dict(os.environ, {
            "HDFS_URL": "http://nn:9870",
            "HDFS_USER": "testuser",
            "HDFS_MODE": "webhdfs",
        }):
            # from_env 会尝试创建真实客户端，我们跳过
            with patch.object(hdfs_utils.HDFSUtils, "_init_client", return_value=MagicMock()):
                hdfs = hdfs_utils.HDFSUtils.from_env()
                assert hdfs.namenode_url == "http://nn:9870"
                assert hdfs.user == "testuser"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
