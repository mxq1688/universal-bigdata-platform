"""
sync/ 数据同步模块 单元测试
覆盖: 模板渲染 / DataX 任务管理 / SyncManager / Kafka 同步
"""
import os
import sys
import json
import tempfile
import pytest
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock, mock_open

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ====================== MySQL 同步测试 ======================

class TestSyncMySQLToODS:
    """MySQL → HDFS 同步测试"""

    def test_render_template(self):
        from sync_mysql_to_ods import render_template

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"host": "${MYSQL_HOST}", "date": "${RUN_DATE}"}')
            f.flush()
            try:
                result = render_template(f.name, {
                    "MYSQL_HOST": "db-server",
                    "RUN_DATE": "2026-03-09",
                })
                data = json.loads(result)
                assert data["host"] == "db-server"
                assert data["date"] == "2026-03-09"
            finally:
                os.unlink(f.name)

    def test_render_template_multiple_placeholders(self):
        from sync_mysql_to_ods import render_template

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"url": "${HDFS_URL}/ods/dt=${RUN_DATE}"}')
            f.flush()
            try:
                result = render_template(f.name, {
                    "HDFS_URL": "hdfs://namenode:9000",
                    "RUN_DATE": "2026-03-09",
                })
                assert "hdfs://namenode:9000/ods/dt=2026-03-09" in result
            finally:
                os.unlink(f.name)

    @patch("sync_mysql_to_ods.subprocess.run")
    def test_sync_table_success(self, mock_run):
        from sync_mysql_to_ods import sync_table

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"job": {"content": [{"reader": {"parameter": {"connection": [{"jdbcUrl": ["jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}"]}]}}}]}}')
            f.flush()
            try:
                result = sync_table(f.name, "orders", "2026-03-09")
                assert result is True
                mock_run.assert_called_once()
            finally:
                os.unlink(f.name)

    def test_sync_table_missing_template(self):
        from sync_mysql_to_ods import sync_table
        result = sync_table("/nonexistent/template.json", "orders", "2026-03-09")
        assert result is False

    @patch("sync_mysql_to_ods.subprocess.run")
    def test_sync_table_failure(self, mock_run):
        from sync_mysql_to_ods import sync_table

        mock_run.return_value = MagicMock(returncode=1, stderr="DataX error")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"test": "${RUN_DATE}"}')
            f.flush()
            try:
                result = sync_table(f.name, "orders", "2026-03-09")
                assert result is False
            finally:
                os.unlink(f.name)

    @patch("sync_mysql_to_ods.subprocess.run")
    def test_sync_table_timeout(self, mock_run):
        import subprocess
        from sync_mysql_to_ods import sync_table

        mock_run.side_effect = subprocess.TimeoutExpired(cmd="datax", timeout=3600)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"test": "${RUN_DATE}"}')
            f.flush()
            try:
                result = sync_table(f.name, "orders", "2026-03-09")
                assert result is False
            finally:
                os.unlink(f.name)

    def test_default_date(self):
        from sync_mysql_to_ods import sync_mysql_to_hdfs
        from datetime import timedelta

        with patch("sync_mysql_to_ods.sync_table", return_value=True):
            with patch("sync_mysql_to_ods.create_hive_partition"):
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                # 不传日期时应该默认用昨天
                result = sync_mysql_to_hdfs(date_str=None, datax_home="/tmp")
                assert result is True


# ====================== SyncManager 测试 ======================

class TestSyncManager:
    """同步管理器测试"""

    def test_init_job_configs(self):
        from sync_manager import SyncManager
        manager = SyncManager(datax_home="/tmp")
        assert "orders" in manager.job_configs
        assert "users" in manager.job_configs
        assert "products" in manager.job_configs
        assert "hdfs2doris" in manager.job_configs

    def test_job_has_correct_structure(self):
        from sync_manager import SyncManager
        manager = SyncManager(datax_home="/tmp")
        for name, config in manager.job_configs.items():
            assert "template" in config
            assert "description" in config
            assert "deps" in config
            assert isinstance(config["deps"], list)

    def test_hdfs2doris_depends_on_tables(self):
        from sync_manager import SyncManager
        manager = SyncManager(datax_home="/tmp")
        deps = manager.job_configs["hdfs2doris"]["deps"]
        assert "orders" in deps
        assert "users" in deps
        assert "products" in deps

    def test_show_jobs(self, capsys):
        from sync_manager import SyncManager
        manager = SyncManager(datax_home="/tmp")
        manager.show_jobs()
        output = capsys.readouterr().out
        assert "orders" in output
        assert "users" in output
        assert "订单" in output

    def test_run_nonexistent_job(self):
        from sync_manager import SyncManager
        manager = SyncManager(datax_home="/tmp")
        result = manager.run_single_job("nonexistent_job")
        assert result is False


# ====================== DataXJob 测试 ======================

class TestDataXJob:
    """DataX 任务管理测试"""

    def test_load_env_from_file(self, tmp_path):
        from sync_manager import DataXJob
        env_file = tmp_path / ".env"
        env_file.write_text("MYSQL_HOST=test-db\nMYSQL_PORT=3307\n")

        job = DataXJob(datax_home="/tmp", env_file=str(env_file))
        assert job.env["MYSQL_HOST"] == "test-db"
        assert job.env["MYSQL_PORT"] == "3307"

    def test_load_env_ignores_comments(self, tmp_path):
        from sync_manager import DataXJob
        env_file = tmp_path / ".env"
        env_file.write_text("# This is a comment\nKEY=value\n")

        job = DataXJob(datax_home="/tmp", env_file=str(env_file))
        assert "KEY" in job.env
        assert "# This is a comment" not in str(job.env)

    def test_load_env_missing_file(self):
        from sync_manager import DataXJob
        job = DataXJob(datax_home="/tmp", env_file="/nonexistent/.env")
        assert job.env == {}

    def test_render_template(self, tmp_path):
        from sync_manager import DataXJob
        job = DataXJob(datax_home="/tmp", env_file="/dev/null")

        template = tmp_path / "job.json"
        template.write_text('{"date": "${RUN_DATE}", "host": "${MYSQL_HOST}"}')

        temp_file = job._render_template(str(template), {
            "RUN_DATE": "2026-03-10",
            "MYSQL_HOST": "localhost",
        })

        try:
            with open(temp_file) as f:
                data = json.load(f)
            assert data["date"] == "2026-03-10"
            assert data["host"] == "localhost"
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_run_job_missing_template(self):
        from sync_manager import DataXJob
        job = DataXJob(datax_home="/tmp", env_file="/dev/null")
        result = job.run_job("/nonexistent/template.json")
        assert result is False


# ====================== Kafka 同步测试 ======================

class TestSyncKafkaToODS:
    """Kafka → HDFS 同步测试"""

    def test_default_date_yesterday(self):
        from sync_kafka_to_ods import sync_kafka_to_ods
        from datetime import timedelta

        with patch("sync_kafka_to_ods.consume_kafka_to_hdfs", return_value=True) as mock_consume:
            with patch("sync_kafka_to_ods.create_hive_partition"):
                result = sync_kafka_to_ods(date_str=None)
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                # 验证传入了昨天的日期
                calls = mock_consume.call_args_list
                for call in calls:
                    assert call.kwargs["date_str"] == yesterday

    def test_sync_two_topics(self):
        from sync_kafka_to_ods import sync_kafka_to_ods

        with patch("sync_kafka_to_ods.consume_kafka_to_hdfs", return_value=True) as mock_consume:
            with patch("sync_kafka_to_ods.create_hive_partition"):
                result = sync_kafka_to_ods(date_str="2026-03-09")
                assert result is True
                # 应该消费 2 个 topic
                assert mock_consume.call_count == 2
                topics = [c.kwargs["topic"] for c in mock_consume.call_args_list]
                assert "user_behavior" in topics
                assert "order_events" in topics

    def test_partial_failure_returns_false(self):
        from sync_kafka_to_ods import sync_kafka_to_ods

        call_count = [0]

        def mock_consume(**kwargs):
            call_count[0] += 1
            return call_count[0] == 1  # 第一个成功，第二个失败

        with patch("sync_kafka_to_ods.consume_kafka_to_hdfs", side_effect=mock_consume):
            with patch("sync_kafka_to_ods.create_hive_partition"):
                result = sync_kafka_to_ods(date_str="2026-03-09")
                assert result is False


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
