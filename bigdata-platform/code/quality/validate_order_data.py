#!/usr/bin/env python3
"""
使用 Great Expectations 进行订单数据质量校验
可独立运行，用于 Airflow DAG 调用
"""
import sys
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ValidateOrder')

try:
    import great_expectations as gx
    from great_expectations.dataset import PandasDataset
    GE_AVAILABLE = True
except ImportError:
    GE_AVAILABLE = False

import pandas as pd


def generate_mock_data(date_str, count=1000):
    """生成模拟订单数据（开发/测试用）"""
    return pd.DataFrame({
        'order_id': [f'ord_{i:05d}' for i in range(1, count + 1)],
        'user_id': [f'user_{(i % 800) + 1:05d}' for i in range(1, count + 1)],
        'product_id': [f'prod_{(i % 200) + 1:04d}' for i in range(1, count + 1)],
        'amount': [round(10 + i * 0.5, 2) for i in range(1, count + 1)],
        'quantity': [i % 5 + 1 for i in range(1, count + 1)],
        'status': ['paid'] * int(count * 0.7) +
                  ['completed'] * int(count * 0.15) +
                  ['shipped'] * int(count * 0.1) +
                  ['refunded'] * (count - int(count * 0.7) - int(count * 0.15) - int(count * 0.1)),
        'order_time': [
            f'{date_str} {(i * 7) % 24:02d}:{(i * 13) % 60:02d}:{(i * 17) % 60:02d}'
            for i in range(1, count + 1)
        ]
    })


def validate_with_ge(df):
    """使用 Great Expectations 校验"""
    ge_df = PandasDataset(df)

    rules = [
        ("order_id 非空", lambda: ge_df.expect_column_values_to_not_be_null("order_id")),
        ("user_id 非空", lambda: ge_df.expect_column_values_to_not_be_null("user_id")),
        ("amount 非空", lambda: ge_df.expect_column_values_to_not_be_null("amount")),
        ("amount 范围", lambda: ge_df.expect_column_values_to_be_between("amount", min_value=0.01, max_value=100000)),
        ("quantity 范围", lambda: ge_df.expect_column_values_to_be_between("quantity", min_value=1, max_value=100)),
        ("order_id 格式", lambda: ge_df.expect_column_values_to_match_regex("order_id", r'^ord_\d{5}$')),
        ("user_id 格式", lambda: ge_df.expect_column_values_to_match_regex("user_id", r'^user_\d{5}$')),
        ("status 枚举", lambda: ge_df.expect_column_values_to_be_in_set(
            "status", ["paid", "created", "shipped", "completed", "refunded"]
        )),
        ("order_id 唯一", lambda: ge_df.expect_column_values_to_be_unique("order_id")),
        ("数据量检查", lambda: ge_df.expect_table_row_count_to_be_between(min_value=1)),
    ]

    return _run_rules(rules)


def validate_with_builtin(df):
    """内置校验（不依赖 GE）"""
    rules = [
        ("order_id 非空", lambda: df["order_id"].notna().all()),
        ("user_id 非空", lambda: df["user_id"].notna().all()),
        ("amount 非空", lambda: df["amount"].notna().all()),
        ("amount > 0", lambda: (df["amount"] > 0).all()),
        ("quantity > 0", lambda: (df["quantity"] > 0).all()),
        ("order_id 格式", lambda: df["order_id"].str.match(r'^ord_\d{5}$').all()),
        ("user_id 格式", lambda: df["user_id"].str.match(r'^user_\d{5}$').all()),
        ("status 枚举", lambda: df["status"].isin(["paid", "created", "shipped", "completed", "refunded"]).all()),
        ("order_id 唯一", lambda: df["order_id"].nunique() == len(df)),
        ("数据量 > 0", lambda: len(df) > 0),
    ]

    return _run_rules(rules)


def _run_rules(rules):
    """执行规则列表并汇总结果"""
    success_count = 0
    fail_count = 0

    for name, check_fn in rules:
        try:
            result = check_fn()

            # GE 返回 dict，内置返回 bool
            if isinstance(result, dict):
                passed = result.get("success", False)
            else:
                passed = bool(result)

            if passed:
                success_count += 1
                logger.info("  ✅ %s: 通过", name)
            else:
                fail_count += 1
                logger.error("  ❌ %s: 失败", name)

        except Exception as e:
            fail_count += 1
            logger.error("  ❌ %s: 异常 — %s", name, e)

    return success_count, fail_count


def validate_order_data(date_str=None):
    """主入口: 验证订单数据质量"""
    if not date_str:
        date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info("🧪 验证日期 %s 的订单数据质量...", date_str)

    # 生成/加载数据
    df = generate_mock_data(date_str)
    logger.info("数据量: %s 条", len(df))

    # 执行校验
    if GE_AVAILABLE:
        logger.info("使用 Great Expectations 校验")
        passed, failed = validate_with_ge(df)
    else:
        logger.info("使用内置校验规则")
        passed, failed = validate_with_builtin(df)

    # 报告
    total = passed + failed
    print("\n" + "=" * 50)
    print(f"📊 数据质量报告 — {date_str}")
    print("=" * 50)
    print(f"  总规则数: {total}")
    print(f"  通过: {passed}")
    print(f"  失败: {failed}")
    print(f"  通过率: {passed / total * 100:.1f}%")
    print("=" * 50)

    if failed == 0:
        print("🎉 所有数据质量规则均通过！")
        return True
    else:
        print("⚠️ 存在数据质量问题，请检查！")
        return False


if __name__ == "__main__":
    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    success = validate_order_data(date_str=date_str)
    sys.exit(0 if success else 1)
