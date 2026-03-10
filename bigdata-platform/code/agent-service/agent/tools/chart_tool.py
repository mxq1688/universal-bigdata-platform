"""
可视化图表工具
"""
import logging
import hashlib
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")  # 非交互后端
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

# 全局中文字体设置
plt.rcParams["font.sans-serif"] = ["SimHei", "DejaVu Sans", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False

# 配色方案
COLORS = [
    "#4E79A7", "#F28E2B", "#E15759", "#76B7B2", "#59A14F",
    "#EDC948", "#B07AA1", "#FF9DA7", "#9C755F", "#BAB0AC",
]


class ChartTool:
    """图表生成器"""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ======================== 核心方法 ========================

    def generate_chart(self, chart_type: str, data: pd.DataFrame, config: dict) -> dict:
        """
        统一图表生成入口
        chart_type: line / bar / pie / scatter / heatmap / funnel / area
        config: {
            "title": str,
            "x": str,          # x 轴字段
            "y": str | list,   # y 轴字段
            "group_by": str,   # 分组字段（可选）
            "sort_by": str,    # 排序字段（可选）
            "top_n": int,      # 只展示 top N（可选）
            "width": int,
            "height": int,
        }
        """
        chart_map = {
            "line": self._line_chart,
            "bar": self._bar_chart,
            "pie": self._pie_chart,
            "scatter": self._scatter_chart,
            "heatmap": self._heatmap_chart,
            "funnel": self._funnel_chart,
            "area": self._area_chart,
        }

        func = chart_map.get(chart_type)
        if not func:
            return {"success": False, "error": f"不支持的图表类型: {chart_type}", "path": ""}

        try:
            # 排序 & Top N
            if config.get("sort_by"):
                data = data.sort_values(config["sort_by"], ascending=False)
            if config.get("top_n"):
                data = data.head(config["top_n"])

            fig, ax = plt.subplots(
                figsize=(config.get("width", 10), config.get("height", 6))
            )

            func(ax, data, config)

            # 标题
            ax.set_title(config.get("title", ""), fontsize=14, fontweight="bold", pad=15)

            plt.tight_layout()

            # 保存
            file_name = self._gen_filename(config.get("title", chart_type))
            file_path = self.output_dir / file_name
            fig.savefig(str(file_path), dpi=150, bbox_inches="tight")
            plt.close(fig)

            logger.info(f"图表已生成: {file_path}")
            return {"success": True, "path": str(file_path), "error": None}

        except Exception as e:
            plt.close("all")
            logger.error(f"图表生成失败: {e}")
            return {"success": False, "path": "", "error": str(e)}

    # ======================== 各类图表 ========================

    def _line_chart(self, ax, data, config):
        x, y = config["x"], config["y"]
        if isinstance(y, list):
            for i, col in enumerate(y):
                ax.plot(data[x], data[col], marker="o", color=COLORS[i % len(COLORS)],
                        label=col, linewidth=2, markersize=4)
            ax.legend()
        else:
            ax.plot(data[x], data[y], marker="o", color=COLORS[0], linewidth=2, markersize=4)
        ax.set_xlabel(x)
        ax.set_ylabel(y if isinstance(y, str) else "")
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha="right")

    def _bar_chart(self, ax, data, config):
        x, y = config["x"], config["y"]
        if isinstance(y, list):
            bar_width = 0.8 / len(y)
            x_pos = np.arange(len(data))
            for i, col in enumerate(y):
                offset = (i - len(y)/2 + 0.5) * bar_width
                ax.bar(x_pos + offset, data[col], bar_width,
                       color=COLORS[i % len(COLORS)], label=col)
            ax.set_xticks(x_pos)
            ax.set_xticklabels(data[x], rotation=45, ha="right")
            ax.legend()
        else:
            bars = ax.bar(data[x], data[y], color=COLORS[:len(data)])
            # 柱子上标注数值
            for bar in bars:
                h = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2, h, f"{h:,.0f}",
                        ha="center", va="bottom", fontsize=9)
            plt.xticks(rotation=45, ha="right")
        ax.set_xlabel(x)
        ax.set_ylabel(y if isinstance(y, str) else "")

    def _pie_chart(self, ax, data, config):
        labels = data[config["x"]]
        values = data[config["y"]]
        colors = COLORS[:len(data)]
        wedges, texts, autotexts = ax.pie(
            values, labels=labels, colors=colors, autopct="%1.1f%%",
            startangle=90, pctdistance=0.85,
        )
        for t in autotexts:
            t.set_fontsize(9)
        ax.axis("equal")

    def _scatter_chart(self, ax, data, config):
        ax.scatter(data[config["x"]], data[config["y"]],
                   c=COLORS[0], alpha=0.6, s=50)
        ax.set_xlabel(config["x"])
        ax.set_ylabel(config["y"])
        ax.grid(True, alpha=0.3)

    def _heatmap_chart(self, ax, data, config):
        numeric = data.select_dtypes(include=[np.number])
        im = ax.imshow(numeric.values, cmap="YlOrRd", aspect="auto")
        ax.set_xticks(range(len(numeric.columns)))
        ax.set_xticklabels(numeric.columns, rotation=45, ha="right")
        if config.get("x") and config["x"] in data.columns:
            ax.set_yticks(range(len(data)))
            ax.set_yticklabels(data[config["x"]])
        plt.colorbar(im, ax=ax)

    def _funnel_chart(self, ax, data, config):
        labels = data[config["x"]].tolist()
        values = data[config["y"]].tolist()
        max_val = max(values) if values else 1
        y_pos = range(len(labels))

        for i, (label, val) in enumerate(zip(labels, values)):
            width = val / max_val
            ax.barh(i, width, color=COLORS[i % len(COLORS)], height=0.6, align="center")
            pct = f"{val/values[0]*100:.1f}%" if values[0] else ""
            ax.text(width + 0.02, i, f"{label}: {val:,.0f} ({pct})", va="center", fontsize=10)

        ax.set_yticks([])
        ax.set_xlim(0, 1.4)
        ax.invert_yaxis()
        ax.set_xlabel("转化比例")

    def _area_chart(self, ax, data, config):
        x, y = config["x"], config["y"]
        if isinstance(y, list):
            for i, col in enumerate(y):
                ax.fill_between(data[x], data[col], alpha=0.3, color=COLORS[i % len(COLORS)], label=col)
                ax.plot(data[x], data[col], color=COLORS[i % len(COLORS)], linewidth=1.5)
            ax.legend()
        else:
            ax.fill_between(data[x], data[y], alpha=0.3, color=COLORS[0])
            ax.plot(data[x], data[y], color=COLORS[0], linewidth=1.5)
        ax.set_xlabel(x)
        ax.set_ylabel(y if isinstance(y, str) else "")
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha="right")

    # ======================== 辅助 ========================

    def _gen_filename(self, title: str) -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        short = hashlib.md5(title.encode()).hexdigest()[:6]
        return f"chart_{ts}_{short}.png"
