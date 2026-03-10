# 大数据平台 - 项目结构说明

## 项目总览
这是一个**企业级大数据平台**，包含从**数据采集、存储、计算、分析**到**调度、监控、治理**的全链路能力，支持 Docker 本地开发、K8s 集群部署、Ansible 裸机部署三种方式。

```
bigdata-platform/           # 主项目目录
├─ 🐳 docker/               # Docker Compose 本地开发环境
│  ├─ docker-compose.yml    # 20+ 大数据组件一键启动
│  ├─ scripts/              # 启动/停止/状态/清理脚本
│  ├─ config/               # Trino/Prometheus/Grafana/Airflow 配置
│  └─ data-generator/       # 模拟数据生成器
│
├─ ☸️ k8s/                   # Kubernetes 生产部署配置
│  ├─ base/                 # Kafka + Flink + Doris + 监控 + Ingress
│  └─ overlays/             # dev 开发环境 / prod 生产环境
│
├─ 🔧 ansible/               # Ansible 裸机部署脚本
│  ├─ inventories/          # 主机清单模板
│  ├─ playbooks/            # 全量部署 Playbook
│  └─ roles/                # Kafka KRaft 配置模板
│
├─ 📦 code/                 # 核心业务代码（实时+离线+调度+质量）
│  ├─ realtime/flink-streaming/   # Java - Flink 实时计算（用户行为分析）
│  ├─ batch/spark-etl/            # Python - Spark 离线 ETL（数仓分层）
│  ├─ airflow/dags/               # Python - Airflow 调度编排（每日ETL）
│  ├─ sync/                       # Python - 数据同步（DataX JSON）
│  └─ quality/                    # Python - 数据质量（Great Expectations）
│
└─ 📖 docs/                 # 文档
```

## 🚀 三种部署方式
| 方式 | 命令 | 适合场景 |
|------|------|----------|
| **Docker Compose** | `cd docker && ./scripts/start.sh` | 本地开发、学习测试 |
| **Kubernetes** | `kubectl apply -k k8s/overlays/dev/` | 有 K8s 集群的团队 |
| **Ansible** | `ansible-playbook -i inventories/production.yml playbooks/deploy-all.yml` | 裸机物理机/VM 服务器 |

## 🧩 核心组件说明
| 组件 | 端口 | 用途 |
|------|------|------|
| Kafka | 9092/8090 | 消息总线（数据管道） |
| Flink | 8081 | 实时计算 |
| Spark | 8080 | 离线计算 |
| Doris | 8030/9030 | OLAP 分析库 |
| Trino | 8082 | 联邦查询引擎 |
| HDFS | 9870 | 分布式存储 |
| ES+Kibana | 9200/5601 | 搜索/日志分析 |
| Airflow | 8180 | 任务调度 |
| Superset | 8088 | BI 报表 |
| Grafana | 3000 | 监控大屏 |
| Prometheus | 9090 | 指标采集 |

## 📊 代码模块说明
| 模块 | 语言 | 功能 |
|------|------|------|
| **Flink 实时计算** | Java | 实时用户行为分析、PV/UV 统计、异常告警 |
| **Spark 离线 ETL** | Python | 数仓 ODS→DWD→DWS→ADS 全链路加工 |
| **Airflow 调度** | Python | 任务编排、定时触发、失败重试、结果通知 |
| **数据同步** | Python+JSON | MySQL→HDFS→Doris→ES 数据搬运 |
| **数据质量** | Python | 数据校验规则、质量报告、异常告警 |

## 🎯 使用步骤
1. **本地开发**：`cd docker && ./scripts/start.sh lite`（轻量版，启动核心组件）
2. **访问各组件**：打开 http://localhost:8090（Kafka UI）、http://localhost:8081（Flink）等
3. **提交计算任务**：Flink 提交 Java Jar、Spark 提交 Python PySpark
4. **查看结果**：Grafana 看实时指标、Superset 看报表

## 🎨 技术栈总结
```
┌─────────────────────────────────────────────────────┐
│  🔧 运维部署：Docker + Kubernetes + Ansible        │
│  🔥 核心计算：Flink（实时）+ Spark（离线）        │
│  🗄️ 存储引擎：Kafka + HDFS + Doris + ES + Redis   │
│  📊 分析工具：Trino + Superset + Grafana          │
│  🧩 开发语言：Java（Flink）+ Python（Spark）      │
│  🛠️ 数据治理：Airflow + Great Expectations       │
└─────────────────────────────────────────────────────┘
```

## 💡 核心优势
1. **开箱即用**：一键启动 20+ 大数据组件，无需复杂配置
2. **三端部署**：Docker 开发、K8s 生产、Ansible 裸机，全面覆盖
3. **全链路能力**：采集→传输→存储→计算→调度→监控→治理一站式
4. **最佳实践**：基于官方推荐架构，企业级可直接生产使用