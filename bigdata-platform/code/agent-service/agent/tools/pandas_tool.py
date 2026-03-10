"""
Pandas 数据分析工具
"""
import io
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class PandasTool:
    """Pandas 数据分析执行器"""

    def execute_code(self, code: str, dataframes: dict[str, pd.DataFrame]) -> dict:
        """
        在沙箱中执行 Pandas 分析代码
        dataframes: {"df_orders": DataFrame, "df_users": DataFrame, ...}
        返回: {"success": bool, "result": Any, "error": str|None, "output": str}
        """
        output_buffer = io.StringIO()

        # 构建安全的执行环境
        safe_globals = {
            "__builtins__": {
                "print": lambda *a, **kw: print(*a, file=output_buffer, **kw),
                "len": len, "range": range, "enumerate": enumerate,
                "zip": zip, "sorted": sorted, "min": min, "max": max,
                "sum": sum, "round": round, "abs": abs, "int": int,
                "float": float, "str": str, "list": list, "dict": dict,
                "tuple": tuple, "set": set, "True": True, "False": False,
                "None": None, "isinstance": isinstance, "type": type,
            },
            "pd": pd,
            "np": np,
        }

        # 注入 DataFrame
        safe_globals.update(dataframes)

        # 存放结果
        safe_globals["_result"] = None

        # 在代码末尾自动捕获最后一个表达式
        exec_code = code.strip()
        if not exec_code.endswith(";"):
            lines = exec_code.split("\n")
            last_line = lines[-1].strip()
            # 如果最后一行是表达式（不是赋值语句），自动赋值给 _result
            if last_line and not any(
                last_line.startswith(kw) for kw in ("import ", "from ", "def ", "class ", "if ", "for ", "while ", "with ", "try:", "#")
            ) and "=" not in last_line.split("(")[0]:
                lines[-1] = f"_result = {last_line}"
                exec_code = "\n".join(lines)

        try:
            exec(exec_code, safe_globals)

            result = safe_globals.get("_result")
            output = output_buffer.getvalue()

            # 如果结果是 DataFrame，转成摘要
            result_summary = self._summarize_result(result)

            logger.info(f"Pandas 代码执行成功, 输出 {len(output)} 字符")
            return {
                "success": True,
                "result": result,
                "result_summary": result_summary,
                "output": output,
                "error": None,
            }

        except Exception as e:
            logger.error(f"Pandas 代码执行失败: {e}")
            return {
                "success": False,
                "result": None,
                "result_summary": "",
                "output": output_buffer.getvalue(),
                "error": str(e),
            }

    # ======================== 内置分析方法 ========================

    def describe_dataframe(self, df: pd.DataFrame) -> str:
        """生成 DataFrame 统计摘要"""
        buf = io.StringIO()
        buf.write(f"形状: {df.shape[0]} 行 × {df.shape[1]} 列\n\n")

        buf.write("字段信息:\n")
        for col in df.columns:
            dtype = df[col].dtype
            nulls = df[col].isnull().sum()
            uniques = df[col].nunique()
            buf.write(f"  {col}: {dtype}, 空值={nulls}, 唯一值={uniques}\n")

        buf.write(f"\n数值列统计:\n{df.describe().to_string()}\n")

        buf.write(f"\n前3行:\n{df.head(3).to_string()}\n")

        return buf.getvalue()

    def correlation_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """相关性分析"""
        numeric_df = df.select_dtypes(include=[np.number])
        return numeric_df.corr()

    def group_analysis(
        self, df: pd.DataFrame, group_col: str, agg_col: str, agg_func: str = "sum"
    ) -> pd.DataFrame:
        """分组聚合分析"""
        return df.groupby(group_col)[agg_col].agg(agg_func).reset_index()

    # ======================== 辅助 ========================

    def _summarize_result(self, result) -> str:
        """将结果转为文字摘要"""
        if result is None:
            return "(无返回值)"
        if isinstance(result, pd.DataFrame):
            if len(result) > 20:
                return f"DataFrame ({result.shape[0]}行×{result.shape[1]}列):\n{result.head(20).to_string()}\n...(共{len(result)}行)"
            return f"DataFrame ({result.shape[0]}行×{result.shape[1]}列):\n{result.to_string()}"
        if isinstance(result, pd.Series):
            return f"Series ({len(result)}条):\n{result.to_string()}"
        return str(result)
