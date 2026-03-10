"""
quality/ 数据质量模块 单元测试
覆盖: OrderDataQualityChecker / validate_order_data / mock 数据生成
"""
import os
import sys
import json
import tempfile
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ====================== Mock 数据生成测试 ======================

class TestMockDataGeneration:
    """Mock 数据生成验证"""

    def test_generate_mock_data(self):
        from validate_order_data import generate_mock_data
        df = generate_mock_data("2026-03-09", count=100)

        assert len(df) == 100
        assert "order_id" in df.columns
        assert "user_id" in df.columns
        assert "amount" in df.columns
        assert "status" in df.columns

    def test_mock_data_order_id_format(self):
        from validate_order_data import generate_mock_data
        df = generate_mock_data("2026-03-09", count=50)
        assert df["order_id"].str.match(r"^ord_\d{5}$").all()

    def test_mock_data_user_id_format(self):
        from validate_order_data import generate_mock_data
        df = generate_mock_data("2026-03-09", count=50)
        assert df["user_id"].str.match(r"^user_\d{5}$").all()

    def test_mock_data_status_values(self):
        from validate_order_data import generate_mock_data
        df = generate_mock_data("2026-03-09", count=1000)
        valid_statuses = {"paid", "created", "shipped", "completed", "refunded"}
        assert set(df["status"].unique()).issubset(valid_statuses)

    def test_mock_data_positive_amounts(self):
        from validate_order_data import generate_mock_data
        df = generate_mock_data("2026-03-09", count=100)
        assert (df["amount"] > 0).all()

    def test_mock_data_unique_order_ids(self):
        from validate_order_data import generate_mock_data
        df = generate_mock_data("2026-03-09", count=500)
        assert df["order_id"].nunique() == 500


# ====================== 内置校验测试 ======================

class TestBuiltinValidation:
    """内置校验规则测试 (不依赖 Great Expectations)"""

    def _make_valid_df(self, count=100):
        return pd.DataFrame({
            "order_id": [f"ord_{i:05d}" for i in range(1, count + 1)],
            "user_id": [f"user_{i:05d}" for i in range(1, count + 1)],
            "product_id": [f"prod_{i:04d}" for i in range(1, count + 1)],
            "amount": [round(10 + i * 0.5, 2) for i in range(1, count + 1)],
            "quantity": [i % 5 + 1 for i in range(1, count + 1)],
            "status": ["paid"] * (count - 10) + ["refunded"] * 10,
            "order_time": [f"2026-03-09 {i % 24:02d}:00:00" for i in range(1, count + 1)],
        })

    def test_valid_data_all_pass(self):
        from validate_order_data import validate_with_builtin
        df = self._make_valid_df()
        passed, failed = validate_with_builtin(df)
        assert failed == 0
        assert passed == 10

    def test_null_order_id_fails(self):
        from validate_order_data import validate_with_builtin
        df = self._make_valid_df()
        df.loc[0, "order_id"] = None
        passed, failed = validate_with_builtin(df)
        assert failed > 0

    def test_duplicate_order_id_fails(self):
        from validate_order_data import validate_with_builtin
        df = self._make_valid_df()
        df.loc[1, "order_id"] = df.loc[0, "order_id"]
        passed, failed = validate_with_builtin(df)
        assert failed > 0

    def test_invalid_status_fails(self):
        from validate_order_data import validate_with_builtin
        df = self._make_valid_df()
        df.loc[0, "status"] = "invalid_status"
        passed, failed = validate_with_builtin(df)
        assert failed > 0

    def test_bad_order_id_format_fails(self):
        from validate_order_data import validate_with_builtin
        df = self._make_valid_df()
        df.loc[0, "order_id"] = "BAD_FORMAT"
        passed, failed = validate_with_builtin(df)
        assert failed > 0

    def test_empty_df_fails(self):
        from validate_order_data import validate_with_builtin
        df = pd.DataFrame({
            "order_id": pd.Series([], dtype=str),
            "user_id": pd.Series([], dtype=str),
            "amount": pd.Series([], dtype=float),
            "quantity": pd.Series([], dtype=int),
            "status": pd.Series([], dtype=str),
        })
        passed, failed = validate_with_builtin(df)
        assert failed > 0  # 数据量 = 0 应该失败


# ====================== OrderDataQualityChecker 测试 ======================

class TestOrderDataQualityChecker:
    """质量检查类测试"""

    def test_init_default_date(self):
        from order_quality_check import OrderDataQualityChecker
        checker = OrderDataQualityChecker()
        assert checker.run_date == pd.Timestamp.now().strftime("%Y-%m-%d")

    def test_init_custom_date(self):
        from order_quality_check import OrderDataQualityChecker
        checker = OrderDataQualityChecker(run_date="2026-01-15")
        assert checker.run_date == "2026-01-15"

    def test_check_demo_passes(self):
        from order_quality_check import OrderDataQualityChecker
        checker = OrderDataQualityChecker()
        result = checker.check_demo()
        assert result is True

    def test_check_demo_has_results(self):
        from order_quality_check import OrderDataQualityChecker
        checker = OrderDataQualityChecker()
        checker.check_demo()
        assert len(checker.results) > 0
        assert all("rule" in r for r in checker.results)
        assert all("success" in r for r in checker.results)

    def test_check_file_csv(self, tmp_path):
        from order_quality_check import OrderDataQualityChecker
        csv_file = tmp_path / "orders.csv"

        df = pd.DataFrame({
            "order_id": [f"ord_{i:05d}" for i in range(1, 51)],
            "user_id": [f"user_{i:05d}" for i in range(1, 51)],
            "product_id": [f"prod_{i:04d}" for i in range(1, 51)],
            "total_amount": [round(50 + i * 1.5, 2) for i in range(1, 51)],
            "quantity": [i % 5 + 1 for i in range(1, 51)],
            "status": ["paid"] * 45 + ["refunded"] * 5,
            "create_time": [f"2026-03-09 {i % 24:02d}:00:00" for i in range(1, 51)],
        })
        df.to_csv(csv_file, index=False)

        checker = OrderDataQualityChecker()
        result = checker.check_file(str(csv_file))
        assert result is True

    def test_check_file_nonexistent(self):
        from order_quality_check import OrderDataQualityChecker
        checker = OrderDataQualityChecker()
        result = checker.check_file("/nonexistent/file.csv")
        assert result is False

    def test_save_report_json(self, tmp_path):
        from order_quality_check import OrderDataQualityChecker

        # 切换到 tmp_path 以免在项目目录创建文件
        orig_dir = os.getcwd()
        os.chdir(tmp_path)
        try:
            checker = OrderDataQualityChecker(run_date="2026-03-09")
            checker.results = [
                {"rule": "test_rule", "success": True},
                {"rule": "test_rule2", "success": False},
            ]
            checker._save_report("test_source")

            report_file = tmp_path / "data_quality_results" / "order_quality_2026-03-09.json"
            assert report_file.exists()

            with open(report_file) as f:
                report = json.load(f)
            assert report["total"] == 2
            assert report["passed"] == 1
            assert report["failed"] == 1
            assert report["source"] == "test_source"
        finally:
            os.chdir(orig_dir)

    def test_print_report(self, capsys):
        from order_quality_check import OrderDataQualityChecker
        checker = OrderDataQualityChecker(run_date="2026-03-09")
        checker._print_report(8, 2)
        output = capsys.readouterr().out
        assert "80.0%" in output
        assert "❌ 存在异常" in output

    def test_print_report_all_pass(self, capsys):
        from order_quality_check import OrderDataQualityChecker
        checker = OrderDataQualityChecker(run_date="2026-03-09")
        checker._print_report(10, 0)
        output = capsys.readouterr().out
        assert "100.0%" in output
        assert "✅ 全部通过" in output


# ====================== validate_order_data 集成测试 ======================

class TestValidateOrderData:
    """集成测试 — validate_order_data"""

    def test_validate_order_data_default(self):
        from validate_order_data import validate_order_data
        result = validate_order_data(date_str="2026-03-09")
        assert result is True

    def test_validate_order_data_no_date(self):
        from validate_order_data import validate_order_data
        result = validate_order_data()
        assert result is True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
