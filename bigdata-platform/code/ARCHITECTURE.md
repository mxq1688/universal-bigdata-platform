# 📊 大数据平台代码架构

## 🧩 代码结构总览

```
bigdata-platform/code/
├── common/                        # 🔧 公共工具类库
│   ├── java-utils/                #    Java 工具 (Maven)
│   │   ├── pom.xml
│   │   └── src/main/java/com/bigdata/common/
│   │       ├── DBUtils.java       #    数据库连接池 (HikariCP)
│   │       ├── KafkaUtils.java    #    Kafka 生产者/消费者
│   │       └── HDFSUtils.java     #    HDFS 文件操作
│   ├── python-utils/              #    Python 工具
│   │   ├── requirements.txt
│   │   ├── db_utils.py            #    数据库工具 (SQLAlchemy)
│   │   ├── hdfs_utils.py          #    HDFS 工具 (WebHDFS/Arrow)
│   │   ├── kafka_utils.py         #    Kafka 工具 (kafka-python)
│   │   └── config_utils.py        #    配置加载 (.env/YAML/JSON)
│   └── scala-utils/               #    Scala 工具 (Spark/Hive)
│       ├── pom.xml
│       └── src/main/scala/com/bigdata/common/
│           ├── SparkUtils.scala   #    Spark ETL 辅助 (分区读写/去重/Profile)
│           └── HiveUtils.scala    #    Hive DDL/分区管理
│
├── sync/                          # 📤 数据同步层 (DataX)
│   ├── requirements.txt
│   ├── sync_manager.py            #    同步任务管理器 (CLI)
│   ├── sync_mysql_to_ods.py       #    MySQL → HDFS ODS 同步
│   ├── sync_kafka_to_ods.py       #    Kafka → HDFS ODS 同步
│   └── datax-jobs/                #    DataX JSON 模板
│       ├── mysql2hdfs.json        #    订单表同步
│       ├── mysql2hdfs_users.json  #    用户表同步
│       ├── mysql2hdfs_products.json#   商品表同步
│       ├── hdfs2doris.json        #    HDFS → Doris 同步
│       ├── kafka2hdfs.json        #    Kafka → HDFS 同步
│       └── mysql2es.json          #    MySQL → Elasticsearch 同步
│
├── realtime/                      # ⚡ 实时计算层 (Flink)
│   └── flink-streaming/           #    Flink Java 项目 (Maven)
│       ├── pom.xml
│       └── src/main/java/com/bigdata/realtime/
│           ├── UserBehaviorRealtime.java   # 用户行为分析: PV/UV + 行为分布 + 热门商品
│           ├── OrderMonitorRealtime.java   # 订单监控: 订单量/GMV + 大额告警 + 退款率
│           ├── bean/UserBehavior.java      # 实体类 + 校验
│           └── util/JsonUtils.java         # JSON 工具
│
├── batch/                         # 📦 离线计算层 (Spark)
│   └── spark-etl/                 #    Spark ETL 项目
│       ├── pom.xml                #    Scala 项目 (Maven)
│       ├── etl_ods_to_dwd.py      #    ODS → DWD 清洗 (PySpark)
│       ├── etl_dwd_to_dws.py      #    DWD → DWS 汇总 (PySpark)
│       ├── etl_dws_to_ads.py      #    DWS → ADS 报表 (PySpark)
│       └── src/main/scala/.../
│           └── DwdTradeOrderDetail.scala  # Scala 版 DWD 加工
│
├── airflow/                       # ⏰ 调度编排层 (Airflow)
│   ├── dags/
│   │   ├── daily_warehouse_etl.py     # 每日数仓 ETL (凌晨 2:00)
│   │   ├── realtime_task_monitor.py   # 实时任务监控 (每 10 分钟)
│   │   └── data_quality_daily.py      # 数据质量巡检 (上午 6:00)
│   └── operators/
│       ├── __init__.py
│       └── custom_operators.py        # DataXOperator + SparkSubmitOperator
│
├── quality/                       # 🧪 数据质量层 (Great Expectations)
│   ├── requirements.txt
│   ├── great_expectations.yml     #    GE 项目配置
│   ├── order_quality_check.py     #    订单质量检查 (支持 Hive/文件/Demo)
│   ├── validate_order_data.py     #    独立校验脚本 (Airflow 可调用)
│   ├── expectations/              #    质量规则集
│   │   ├── order_data_suite.json
│   │   └── user_behavior_suite.json
│   └── checkpoints/               #    校验任务
│       ├── daily_order_checkpoint.json
│       └── daily_behavior_checkpoint.json
│
├── agent-service/                 # 🤖 智能分析层 (LLM Agent)
│   ├── requirements.txt
│   ├── .env / .env.example
│   ├── main.py                    #    CLI 入口
│   ├── agent/                     #    Agent 核心
│   │   ├── core.py                #    ReAct 循环引擎
│   │   ├── llm.py                 #    LLM 封装 (OpenAI/Anthropic)
│   │   ├── tools/                 #    工具集 (SQL/Pandas/Chart/Metric)
│   │   ├── prompts/               #    System Prompt
│   │   └── knowledge/             #    业务知识库
│   ├── config/knowledge.yaml      #    表结构 + 指标 + 术语映射
│   ├── web/                       #    FastAPI Web 服务 + UI
│   ├── tests/                     #    单元测试
│   └── scripts/start.sh           #    启动脚本
│
├── sql/                           # 📝 数仓 DDL
│   ├── init/
│   │   └── 01_init_mysql.sql      #    MySQL 业务表 (Docker 初始化)
│   ├── hive_ods_ddl.sql           #    Hive ODS 层建表
│   ├── doris_dwd_ddl.sql          #    Doris DWD 明细层建表
│   ├── doris_dws_ddl.sql          #    Doris DWS 汇总层建表
│   └── doris_ads_ddl.sql          #    Doris ADS 报表层建表
│
├── scripts/                       # 🔨 运维工具
│   ├── generate_mock_data.py      #    Mock 数据生成 (MySQL/Kafka/CSV)
│   └── monitor.py                 #    平台监控告警 (Flink/Airflow/数仓/磁盘)
│
├── monitoring/                    # 📡 监控体系 (Prometheus + Grafana)
│   ├── prometheus/
│   │   ├── prometheus.yml         #    Prometheus 主配置 (9 个采集目标)
│   │   └── rules/                 #    告警规则
│   │       ├── infra_alerts.yml   #    基础设施告警 (CPU/内存/磁盘/MySQL/Redis/ES)
│   │       └── bigdata_alerts.yml #    业务组件告警 (Kafka/Flink/Airflow/Doris/Agent)
│   ├── alertmanager/
│   │   └── alertmanager.yml       #    Alertmanager 路由 + 分级告警
│   └── grafana/
│       ├── grafana.env            #    Grafana 环境配置
│       ├── provisioning/          #    数据源 + Dashboard 自动发现
│       │   ├── datasources/datasources.yml
│       │   └── dashboards/dashboards.yml
│       └── dashboards/            #    预置仪表板 (5 个)
│           ├── 01-platform-overview.json   # 平台总览
│           ├── 02-kafka.json               # Kafka 集群
│           ├── 03-flink.json               # Flink 实时计算
│           ├── 04-doris.json               # Doris 数仓
│           └── 05-etl-agent.json           # ETL 链路 + Agent 分析
│
├── docker-compose.yml             # 🐳 本地开发环境
├── docker-compose.monitoring.yml  # 📡 监控栈 (Prometheus + Grafana + Exporters)
├── Makefile                       # 🚀 一键操作命令
├── .env.example                   # ⚙️ 环境变量模板
├── .gitignore                     # 🚫 Git 忽略规则
├── README.md                      # 📖 项目 README
├── ARCHITECTURE.md                # 本文件
└── SYSTEM_ARCHITECTURE.md         # 模块间关系图
```

## 🛠️ 技术选型

| 层 | 技术 | 语言 | 用途 |
|----|------|------|------|
| 实时计算 | Flink 1.19 | Java 11 | 用户行为实时分析、窗口聚合 |
| 离线计算 | Spark 3.5 | Scala 2.12 / PySpark | 数仓分层 ETL (ODS→DWD→DWS→ADS) |
| 数据同步 | DataX | Python + JSON | MySQL/Kafka → HDFS/Doris 数据搬运 |
| 任务调度 | Airflow | Python | DAG 编排、依赖管理、监控告警 |
| 数据质量 | Great Expectations | Python | 字段校验、空值检查、格式验证 |
| 智能分析 | LLM + ReAct Agent | Python | 自然语言 → SQL → 分析 → 图表 |
| 工具类库 | HikariCP / SQLAlchemy | Java + Python | 连接池、Kafka、HDFS、配置管理 |
| 数据存储 | HDFS + Doris + ES | - | 原始数据 + 数仓 + 全文检索 |
| 可观测性 | Prometheus + Grafana | YAML + JSON | 指标采集 + 可视化仪表板 + 告警 |

## 🚀 运行方式

### Flink 实时任务
```bash
flink run -m flink-jobmanager:8081 \
  -c com.bigdata.realtime.UserBehaviorRealtime \
  flink-streaming-1.0.jar
```

### Spark ETL
```bash
spark-submit --master yarn --deploy-mode client \
  etl_ods_to_dwd.py 2026-03-09
```

### DataX 同步
```bash
cd sync && python sync_mysql_to_ods.py 2026-03-09
cd sync && python sync_manager.py daily --date 2026-03-09
```

### Airflow 调度
```bash
# DAG 文件放到 $AIRFLOW_HOME/dags/ 即自动加载
cp airflow/dags/*.py $AIRFLOW_HOME/dags/
```

### 数据质量检查
```bash
cd quality && python order_quality_check.py demo
cd quality && python validate_order_data.py 2026-03-09
```

### Agent 智能分析
```bash
cd agent-service && python main.py                  # 交互模式
cd agent-service && python main.py -q "今天GMV多少"  # 单次查询
cd agent-service && python main.py --web             # Web 服务
```

## 📊 数据流链路

```
MySQL/Kafka/日志
    ↓ (sync/ DataX)
HDFS ODS 层
    ↓ (batch/ Spark)
Hive DWD 明细层
    ↓ (batch/ Spark)
Doris DWS 汇总层
    ↓ (batch/ Spark)
Doris ADS 报表层
    ↓
BI 报表 / Agent 智能分析

同时:
Kafka → Flink (realtime/) → Doris 实时指标
Airflow (airflow/) 调度所有任务
Quality (quality/) 校验所有数据
Common (common/) 支撑所有模块
```
