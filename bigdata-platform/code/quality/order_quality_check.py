#!/usr/bin/env python3
"""
订单表数据质量检查
使用 Great Expectations 定义数据质量规则

用法:
  python order_quality_check.py hive 20260101
  python order_quality_check.py file orders.csv
  python order_quality_check.py demo              # 使用 mock 数据演示
"""
import sys
import os
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('OrderQualityCheck')

try:
    import great_expectations as gx
    from great_expectations.dataset import PandasDataset
    GE_AVAILABLE = True
except ImportError:
    GE_AVAILABLE = False
    logger.warning("great_expectations 未安装，使用内置检查逻辑")

try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


class OrderDataQualityChecker:
    """订单数据质量检查类"""

    def __init__(self, run_date=None):
        self.run_date = run_date or datetime.now().strftime('%Y-%m-%d')
        self.results = []

    def _define_expectations(self, ge_df):
        """在 GE DataFrame 上定义质量规则"""
        expectations = []

        # 1. 主键非空、唯一
        expectations.append(ge_df.expect_column_values_to_not_be_null("order_id"))
        expectations.append(ge_df.expect_column_values_to_be_unique("order_id"))

        # 2. 关键字段非空
        expectations.append(ge_df.expect_column_values_to_not_be_null("user_id"))
        expectations.append(ge_df.expect_column_values_to_not_be_null("total_amount"))
        expectations.append(ge_df.expect_column_values_to_not_be_null("status"))

        # 3. 数值范围
        expectations.append(ge_df.expect_column_values_to_be_between(
            "total_amount", min_value=0, strict_min=False
        ))

        # 4. 状态枚举值
        expectations.append(ge_df.expect_column_values_to_be_in_set(
            "status", ["created", "paid", "shipped", "completed", "refunded"]
        ))

        # 5. ID 格式
        expectations.append(ge_df.expect_column_values_to_match_regex(
            "order_id", r'^ord_\d+'
        ))
        expectations.append(ge_df.expect_column_values_to_match_regex(
            "user_id", r'^user_\d+'
        ))

        # 6. 数据量 > 0
        expectations.append(ge_df.expect_table_row_count_to_be_between(min_value=1))

        return expectations

    def check_with_ge(self, df):
        """使用 Great Expectations 检查 Pandas DataFrame"""
        import pandas as pd

        if not GE_AVAILABLE:
            logger.warning("GE 不可用，回退到内置检查")
            return self.check_with_builtin(df)

        ge_df = PandasDataset(df)
        expectations = self._define_expectations(ge_df)

        passed = 0
        failed = 0

        for exp in expectations:
            success = exp.get("success", False)
            exp_type = exp.get("expectation_config", {}).get("expectation_type", "unknown")
            kwargs = exp.get("expectation_config", {}).get("kwargs", {})
            column = kwargs.get("column", "table")

            result_entry = {
                "rule": exp_type,
                "column": column,
                "success": success,
            }

            if success:
                passed += 1
                logger.info("  ✅ %s.%s: 通过", column, exp_type)
            else:
                failed += 1
                result_info = exp.get("result", {})
                result_entry["detail"] = str(result_info)
                logger.error("  ❌ %s.%s: 失败 — %s", column, exp_type, result_info)

            self.results.append(result_entry)

        return passed, failed

    def check_with_builtin(self, df):
        """内置检查逻辑（不依赖 GE）"""
        import pandas as pd

        rules = [
            ("order_id 非空", df["order_id"].notna().all()),
            ("order_id 唯一", df["order_id"].nunique() == len(df)),
            ("user_id 非空", df["user_id"].notna().all()),
            ("total_amount 非空", df["total_amount"].notna().all()),
            ("total_amount >= 0", (df["total_amount"] >= 0).all()),
            ("status 枚举值", df["status"].isin(["created", "paid", "shipped", "completed", "refunded"]).all()),
            ("order_id 格式", df["order_id"].str.match(r'^ord_\d+').all()),
            ("user_id 格式", df["user_id"].str.match(r'^user_\d+').all()),
            ("数据量 > 0", len(df) > 0),
        ]

        passed = 0
        failed = 0

        for name, ok in rules:
            self.results.append({"rule": name, "success": bool(ok)})
            if ok:
                passed += 1
                logger.info("  ✅ %s: 通过", name)
            else:
                failed += 1
                logger.error("  ❌ %s: 失败", name)

        return passed, failed

    def check_hive_table(self, table_name="dwd.dwd_trade_order_detail"):
        """检查 Hive 表"""
        if not SPARK_AVAILABLE:
            logger.error("Spark 不可用，无法检查 Hive 表")
            return False

        spark = SparkSession.builder \
            .appName(f"OrderDataQuality_{self.run_date}") \
            .enableHiveSupport() \
            .getOrCreate()

        try:
            df = spark.sql(
                f"SELECT * FROM {table_name} WHERE dt = '{self.run_date}'"
            ).toPandas()

            logger.info("从 Hive 读取 %s 条数据", len(df))
            passed, failed = self.check_with_ge(df) if GE_AVAILABLE else self.check_with_builtin(df)
            self._print_report(passed, failed)
            self._save_report(table_name)
            return failed == 0

        except Exception as e:
            logger.error("Hive 表检查失败: %s", e)
            return False
        finally:
            spark.stop()

    def check_file(self, file_path):
        """检查 CSV 文件"""
        import pandas as pd

        if not os.path.exists(file_path):
            logger.error("文件不存在: %s", file_path)
            return False

        df = pd.read_csv(file_path)
        logger.info("从文件读取 %s 条数据", len(df))

        passed, failed = self.check_with_ge(df) if GE_AVAILABLE else self.check_with_builtin(df)
        self._print_report(passed, failed)
        self._save_report(file_path)
        return failed == 0

    def check_demo(self):
        """使用 mock 数据演示检查"""
        import pandas as pd

        df = pd.DataFrame({
            'order_id': [f'ord_{i:05d}' for i in range(1, 101)],
            'user_id': [f'user_{i:05d}' for i in range(1, 101)],
            'product_id': [f'prod_{i:04d}' for i in range(1, 101)],
            'total_amount': [round(50 + i * 1.5, 2) for i in range(1, 101)],
            'quantity': [i % 5 + 1 for i in range(1, 101)],
            'status': ['paid'] * 90 + ['refunded'] * 10,
            'create_time': [f'{self.run_date} {i % 24:02d}:{i % 60:02d}:00' for i in range(1, 101)],
        })

        logger.info("使用 mock 数据 (%s 条) 进行演示检查", len(df))
        passed, failed = self.check_with_ge(df) if GE_AVAILABLE else self.check_with_builtin(df)
        self._print_report(passed, failed)
        return failed == 0

    def _print_report(self, passed, failed):
        """打印检查报告"""
        total = passed + failed
        print("\n" + "=" * 60)
        print(f"📊 数据质量检查报告 — {self.run_date}")
        print("=" * 60)
        print(f"  总规则数: {total}")
        print(f"  通过: {passed}")
        print(f"  失败: {failed}")
        print(f"  通过率: {passed / total * 100:.1f}%" if total > 0 else "  无检查规则")
        print(f"  结果: {'✅ 全部通过' if failed == 0 else '❌ 存在异常'}")
        print("=" * 60 + "\n")

    def _save_report(self, source):
        """保存报告为 JSON"""
        output_dir = "data_quality_results"
        os.makedirs(output_dir, exist_ok=True)

        report = {
            "source": source,
            "run_date": self.run_date,
            "check_time": datetime.now().isoformat(),
            "total": len(self.results),
            "passed": sum(1 for r in self.results if r["success"]),
            "failed": sum(1 for r in self.results if not r["success"]),
            "results": self.results,
        }

        output_file = os.path.join(output_dir, f"order_quality_{self.run_date}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger.info("报告已保存: %s", output_file)


def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  python order_quality_check.py demo")
        print("  python order_quality_check.py hive [20260101]")
        print("  python order_quality_check.py file orders.csv")
        sys.exit(1)

    check_type = sys.argv[1]
    checker = OrderDataQualityChecker(
        run_date=sys.argv[2] if len(sys.argv) > 2 and check_type == "hive" else None
    )

    if check_type == "demo":
        success = checker.check_demo()
    elif check_type == "hive":
        success = checker.check_hive_table()
    elif check_type == "file":
        if len(sys.argv) < 3:
            print("请指定文件路径")
            sys.exit(1)
        success = checker.check_file(sys.argv[2])
    else:
        print(f"不支持的检查类型: {check_type}")
        sys.exit(1)

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
