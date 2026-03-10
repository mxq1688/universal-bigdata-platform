"""
scripts/ 运维工具 单元测试
覆盖: Mock 数据生成器 / 监控告警工具
"""
import os
import sys
import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ====================== Mock 数据生成器测试 ======================

class TestGenerateMockData:
    """Mock 数据生成器测试"""

    def test_generate_users(self):
        from generate_mock_data import generate_users
        users = generate_users(count=100)
        assert len(users) == 100
        assert all("user_id" in u for u in users)
        assert all("username" in u for u in users)
        assert all("city" in u for u in users)
        assert all(u["user_id"].startswith("user_") for u in users)

    def test_generate_users_unique_ids(self):
        from generate_mock_data import generate_users
        users = generate_users(count=500)
        ids = [u["user_id"] for u in users]
        assert len(ids) == len(set(ids))

    def test_generate_users_valid_age(self):
        from generate_mock_data import generate_users
        users = generate_users(count=200)
        assert all(18 <= u["age"] <= 65 for u in users)

    def test_generate_users_valid_gender(self):
        from generate_mock_data import generate_users
        users = generate_users(count=200)
        assert all(u["gender"] in (0, 1, 2) for u in users)

    def test_generate_products(self):
        from generate_mock_data import generate_products
        products = generate_products(count=50)
        assert len(products) == 50
        assert all("product_id" in p for p in products)
        assert all("category" in p for p in products)
        assert all("price" in p for p in products)
        assert all(p["price"] > 0 for p in products)

    def test_generate_products_cost_less_than_price(self):
        from generate_mock_data import generate_products
        products = generate_products(count=100)
        assert all(p["cost"] <= p["price"] for p in products)

    def test_generate_stores(self):
        from generate_mock_data import generate_stores
        stores = generate_stores(count=20)
        assert len(stores) == 20
        assert all("store_id" in s for s in stores)
        assert all("city" in s for s in stores)
        assert all(s["status"] == "open" for s in stores)

    def test_generate_orders(self):
        from generate_mock_data import generate_orders
        orders = generate_orders(count=500)
        assert len(orders) == 500
        assert all("order_id" in o for o in orders)
        assert all("user_id" in o for o in orders)
        assert all("total_amount" in o for o in orders)
        assert all("status" in o for o in orders)

    def test_generate_orders_valid_amounts(self):
        from generate_mock_data import generate_orders
        orders = generate_orders(count=200)
        for o in orders:
            assert o["total_amount"] > 0
            assert o["pay_amount"] > 0
            assert o["pay_amount"] <= o["total_amount"]
            assert o["discount_amount"] >= 0

    def test_generate_orders_valid_status(self):
        from generate_mock_data import generate_orders
        valid = {"created", "paid", "shipped", "completed", "refunded"}
        orders = generate_orders(count=1000)
        assert all(o["status"] in valid for o in orders)

    def test_generate_orders_pay_time_logic(self):
        from generate_mock_data import generate_orders
        orders = generate_orders(count=100)
        for o in orders:
            if o["status"] == "created":
                assert o["pay_time"] is None
            else:
                assert o["pay_time"] is not None

    def test_generate_user_behaviors(self):
        from generate_mock_data import generate_user_behaviors
        behaviors = generate_user_behaviors(count=1000)
        assert len(behaviors) == 1000
        assert all("user_id" in b for b in behaviors)
        assert all("action" in b for b in behaviors)
        assert all("product_id" in b for b in behaviors)

    def test_generate_behaviors_valid_actions(self):
        from generate_mock_data import generate_user_behaviors
        valid = {"view", "click", "cart", "order", "pay", "search", "favorite"}
        behaviors = generate_user_behaviors(count=500)
        assert all(b["action"] in valid for b in behaviors)

    def test_write_to_csv(self, tmp_path):
        from generate_mock_data import generate_users, generate_products, \
            generate_stores, generate_orders, generate_user_behaviors, write_to_csv

        users = generate_users(10)
        products = generate_products(5)
        stores = generate_stores(3)
        orders = generate_orders(20, 10, 5, 3)
        behaviors = generate_user_behaviors(50, 10, 5)

        orig_dir = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = write_to_csv(users, products, stores, orders, behaviors)
            assert result is True

            for fname in ["users.csv", "products.csv", "stores.csv", "orders.csv", "user_behaviors.csv"]:
                fpath = tmp_path / "mock_data" / fname
                assert fpath.exists(), f"{fname} should exist"
                assert fpath.stat().st_size > 0, f"{fname} should not be empty"
        finally:
            os.chdir(orig_dir)


# ====================== 监控告警测试 ======================

class TestMonitor:
    """监控告警工具测试"""

    def test_generate_report_all_ok(self):
        from monitor import generate_report
        results = {
            "Flink 实时任务": True,
            "Airflow 调度": True,
            "数仓数据延迟": True,
            "存储空间": True,
        }
        report = generate_report(results)
        assert "🟢 全部正常" in report
        assert "✅" in report
        assert "❌" not in report

    def test_generate_report_with_failures(self):
        from monitor import generate_report
        results = {
            "Flink 实时任务": True,
            "Airflow 调度": False,
            "数仓数据延迟": True,
            "存储空间": False,
        }
        report = generate_report(results)
        assert "🔴 存在异常" in report
        assert "❌" in report

    @patch("monitor.urlopen")
    def test_check_flink_running(self, mock_urlopen):
        from monitor import check_flink

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "jobs": [
                {"jid": "abc12345", "name": "UserBehaviorRealtime", "state": "RUNNING"},
                {"jid": "def67890", "name": "OrderMonitorRealtime", "state": "RUNNING"},
            ]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = lambda s, *a: None
        mock_urlopen.return_value = mock_response

        result = check_flink()
        assert result is True

    @patch("monitor.urlopen")
    def test_check_flink_failed_job(self, mock_urlopen):
        from monitor import check_flink

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "jobs": [
                {"jid": "abc12345", "name": "FailingJob", "state": "FAILED"},
            ]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = lambda s, *a: None
        mock_urlopen.return_value = mock_response

        with patch("monitor.send_alert"):
            result = check_flink()
            assert result is False

    @patch("monitor.urlopen")
    def test_check_flink_unreachable(self, mock_urlopen):
        from urllib.error import URLError
        from monitor import check_flink

        mock_urlopen.side_effect = URLError("Connection refused")

        with patch("monitor.send_alert"):
            result = check_flink()
            assert result is False

    @patch("monitor.urlopen")
    def test_check_flink_no_jobs(self, mock_urlopen):
        from monitor import check_flink

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"jobs": []}).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = lambda s, *a: None
        mock_urlopen.return_value = mock_response

        result = check_flink()
        assert result is True

    @patch("monitor.subprocess.run")
    def test_check_disk_healthy(self, mock_run):
        from monitor import check_disk

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Filesystem      Size  Used Avail Use% Mounted on\n"
                   "/dev/sda1       100G   40G   60G  40% /\n"
        )

        result = check_disk()
        assert result is True

    @patch("monitor.subprocess.run")
    def test_check_disk_critical(self, mock_run):
        from monitor import check_disk

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Filesystem      Size  Used Avail Use% Mounted on\n"
                   "/dev/sda1       100G   95G    5G  95% /\n"
        )

        with patch("monitor.send_alert"):
            result = check_disk()
            assert result is False

    def test_send_webhook(self):
        from monitor import _send_webhook

        with patch("monitor.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = b'{"errcode":0}'
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = lambda s, *a: None
            mock_urlopen.return_value = mock_resp

            _send_webhook("http://example.com/webhook", "Test Alert", "Test Content", "warning")
            mock_urlopen.assert_called_once()

    def test_send_alert_no_channels(self):
        """没有配置告警通道时不应报错"""
        from monitor import send_alert

        with patch.dict(os.environ, {}, clear=True):
            # 不应抛异常
            send_alert("Test", "Content", "info")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
