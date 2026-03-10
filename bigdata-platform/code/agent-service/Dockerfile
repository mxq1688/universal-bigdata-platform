# =============================================
# Data Analysis Agent Service — Docker 镜像
# =============================================
FROM python:3.11-slim

LABEL maintainer="bigdata team"
LABEL description="Data Analysis Agent - LLM 驱动的智能数据分析"

WORKDIR /app

# 系统依赖 (matplotlib 中文字体)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        fonts-noto-cjk curl && \
    rm -rf /var/lib/apt/lists/*

# Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 应用代码
COPY . .

# 输出目录
RUN mkdir -p /app/output/charts /app/output/reports

# 健康检查
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8501/health || exit 1

EXPOSE 8501

# 默认启动 Web 服务
CMD ["python", "main.py", "--web"]
