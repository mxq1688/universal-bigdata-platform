# 📊 大数据分析平台

> 一站式企业级大数据平台，覆盖数据同步 → 实时/离线计算 → 数仓分层 → 数据质量 → 智能分析全链路。

## ⚡ 快速开始

### 环境要求

| 组件 | 版本 | 用途 |
|------|------|------|
| JDK | 11+ | Flink / Spark / DataX |
| Python | 3.10+ | PySpark / Airflow / Agent |
| Scala | 2.12 | Spark ETL |
| Maven | 3.8+ | Java/Scala 项目构建 |
| Docker | 20+ | 本地开发环境 |

### 一键启动本地环境

```bash
# 1. 复制环境变量
cp .env.example .env
# 编辑 .env 填入实际配置

# 2. 启动基础设施 (Kafka, HDFS, Doris, ES, Airflow)
docker-compose up -d

# 3. 构建 Java/Scala 项目
make build

# 4. 运行数据同步（示例）
make sync-daily DATE=2026-03-09

# 5. 运行离线 ETL
make etl-daily DATE=2026-03-09

# 6. 运行数据质量检查
make quality-check

# 7. 启动 Agent 智能分析
make agent-web
```

## 🧩 项目架构

```
├── common/                # 🔧 公共工具类库 (Java + Python + Scala)
├── sync/                  # 📤 数据同步层 (DataX + Python)
├── realtime/              # ⚡ 实时计算层 (Flink)
├── batch/                 # 📦 离线计算层 (Spark)
├── airflow/               # ⏰ 调度编排层 (Airflow)
├── quality/               # 🧪 数据质量层 (Great Expectations)
├── agent-service/         # 🤖 智能分析层 (LLM Agent)
├── monitoring/            # 📡 监控体系 (Prometheus + Grafana)
├── sql/                   # 📝 数仓 DDL 建表语句
├── docker-compose.yml     # 🐳 本地开发环境
├── docker-compose.monitoring.yml  # 📡 监控栈编排
├── Makefile               # 🚀 一键操作命令
└── .env.example           # ⚙️ 环境变量模板
```

## 📊 数据流链路

```
┌──────────────────────────────────────────────────────────────────────┐
│                          数据源                                      │
│   MySQL (业务库)    Kafka (行为日志)    文件 (CSV/日志)               │
└──────────┬──────────────┬──────────────────┬────────────────────────┘
           │              │                  │
           ▼              ▼                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     sync/ 数据同步层                                  │
│   DataX: MySQL→HDFS  |  Python: Kafka→HDFS  |  DataX: MySQL→ES      │
└──────────┬──────────────┬──────────────────┬────────────────────────┘
           │              │                  │
     ┌─────▼─────┐  ┌────▼────┐    ┌───────▼───────┐
     │   HDFS    │  │  Kafka  │    │ Elasticsearch │
     │  (ODS层)  │  │  (流式)  │    │   (全文检索)   │
     └─────┬─────┘  └────┬────┘    └───────────────┘
           │              │
     ┌─────▼─────┐  ┌────▼──────────┐
     │  batch/   │  │   realtime/   │
     │  Spark    │  │   Flink       │
     │ ODS→DWD   │  │ 用户行为分析  │
     │ DWD→DWS   │  │ 订单实时监控  │
     │ DWS→ADS   │  │              │
     └─────┬─────┘  └──────┬───────┘
           │               │
           ▼               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         Doris 数仓                                   │
│   DWD 明细层  |  DWS 汇总层  |  ADS 报表层  |  实时指标层            │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ airflow/ │ │ quality/ │ │  agent/  │
        │ 任务调度  │ │ 质量巡检  │ │ 智能分析  │
        └──────────┘ └──────────┘ └──────────┘
```

## 🛠️ 各模块说明

### 📤 sync/ — 数据同步

将外部数据源同步到大数据平台存储层。

```bash
# MySQL 全量同步
python sync/sync_mysql_to_ods.py 2026-03-09

# Kafka 消费写 HDFS
python sync/sync_kafka_to_ods.py 2026-03-09

# 同步管理器
python sync/sync_manager.py daily --date 2026-03-09
python sync/sync_manager.py list
```

### ⚡ realtime/ — Flink 实时计算

| 任务 | 功能 | 输入 | 输出 |
|------|------|------|------|
| UserBehaviorRealtime | PV/UV、行为分布、热门商品 | Kafka: user_behavior | Doris: dws_realtime_* |
| OrderMonitorRealtime | 订单量/GMV、大额告警、退款率 | Kafka: order_events | Doris: dws_realtime_order_monitor_1min |

```bash
# 提交 Flink 任务
flink run -c com.bigdata.realtime.UserBehaviorRealtime flink-streaming-1.0.jar
flink run -c com.bigdata.realtime.OrderMonitorRealtime flink-streaming-1.0.jar
```

### 📦 batch/ — Spark 离线 ETL

数仓分层加工：ODS → DWD → DWS → ADS

```bash
spark-submit batch/spark-etl/etl_ods_to_dwd.py 2026-03-09
spark-submit batch/spark-etl/etl_dwd_to_dws.py 2026-03-09
spark-submit batch/spark-etl/etl_dws_to_ads.py 2026-03-09
```

### ⏰ airflow/ — 任务调度

| DAG | 调度时间 | 功能 |
|-----|---------|------|
| daily_warehouse_etl | 每天 02:00 | 数据同步 → ETL → DWD → DWS → ADS |
| data_quality_daily | 每天 06:00 | DWD/DWS/ADS 质量巡检 |
| realtime_task_monitor | 每 10 分钟 | Flink 任务健康监控 |

### 🧪 quality/ — 数据质量

基于 Great Expectations 的数据质量框架。

```bash
python quality/order_quality_check.py demo      # 演示模式
python quality/validate_order_data.py 2026-03-09 # 校验订单数据
```

### 🤖 agent-service/ — 智能分析 Agent

LLM + ReAct Agent，自然语言驱动的数据分析。

```bash
# 交互模式
python agent-service/main.py

# 单次查询
python agent-service/main.py -q "过去7天GMV趋势如何？"

# Web 服务
python agent-service/main.py --web
# 访问 http://localhost:8501
```

## 📡 监控体系 (Prometheus + Grafana)

完整的可观测性体系，覆盖基础设施 → 数据组件 → 业务链路三个层级。

### 一键启动

```bash
# 启动监控栈 (Prometheus + Grafana + Alertmanager + 7 个 Exporter)
make monitor-up

# 全部启动 (开发环境 + 监控)
make all-up
```

### 访问地址

| 服务 | 地址 | 账号 |
|------|------|------|
| Grafana | http://localhost:3000 | admin / bigdata2026 |
| Prometheus | http://localhost:9090 | — |
| Alertmanager | http://localhost:9093 | — |

### 预置仪表板 (5 个)

| 仪表板 | 内容 | 面板数 |
|--------|------|--------|
| 📊 平台总览 | 服务健康 + CPU/内存/磁盘 + 网络IO + 告警数 | 10 |
| 📮 Kafka 集群 | Broker 状态 + 消息速率 + Consumer Lag + ISR | 8 |
| ⚡ Flink 实时计算 | 作业状态 + 吞吐量 + Checkpoint + 反压 + GC | 12 |
| 🏛️ Doris 数仓 | FE/BE 状态 + QPS + 延迟分位 + Compaction + Stream Load | 12 |
| 🔄 ETL & Agent | DAG 状态 + 任务耗时 + Agent 请求量 + 各层数据量 | 10 |

### 告警规则 (30+ 条)

**基础设施**: CPU > 85% / 内存 > 85% / 磁盘 > 80% / 节点宕机 / MySQL 慢查询

**数据组件**: Kafka Lag > 10w / Flink 作业失败 / Checkpoint 超时 / Doris FE/BE 宕机

**业务链路**: Airflow DAG 失败 / Agent 错误率 > 30% / 分析耗时 > 30s

### 告警路由

```
Critical → 立即通知 (企微 + 邮件) + 1h 重复
Warning  → 聚合后通知 (企微) + 4h 重复
按 team 分发: data → 数据团队 / infra → 基础设施团队
```

## ⚙️ 环境变量

参见 [.env.example](.env.example)，核心配置项：

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `MYSQL_HOST` | MySQL 地址 | localhost |
| `MYSQL_PORT` | MySQL 端口 | 3306 |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka 集群 | kafka:9092 |
| `HDFS_URL` | HDFS NameNode | hdfs://namenode:8020 |
| `DORIS_FE_HOST` | Doris FE | doris-fe |
| `ES_HOST` | Elasticsearch | localhost |
| `OPENAI_API_KEY` | LLM API Key | — |

## 📐 技术选型

| 层 | 技术 | 语言 | 版本 |
|----|------|------|------|
| 实时计算 | Apache Flink | Java 11 | 1.19 |
| 离线计算 | Apache Spark | Scala 2.12 / PySpark | 3.5 |
| 数据同步 | DataX | Python + JSON | — |
| 任务调度 | Apache Airflow | Python | 2.8+ |
| 数据质量 | Great Expectations | Python | 0.18+ |
| 数据仓库 | Apache Doris | SQL | 2.0+ |
| 原始存储 | Apache HDFS | — | 3.3+ |
| 全文检索 | Elasticsearch | — | 8.x |
| 消息队列 | Apache Kafka | — | 3.6+ |
| 智能分析 | LLM + ReAct Agent | Python | — |
| 可观测性 | Prometheus + Grafana | YAML + JSON | 2.51 / 10.4 |
| 工具类库 | HikariCP / SQLAlchemy | Java + Python | — |

## 📄 License

内部项目，仅供团队使用。
