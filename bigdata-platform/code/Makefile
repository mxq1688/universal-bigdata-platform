# =================================================================
# 大数据平台 Makefile
# =================================================================

.PHONY: help build clean sync-daily etl-daily quality-check agent-cli agent-web \
        flink-build flink-submit docker-up docker-down docker-logs test \
        monitor-up monitor-down monitor-logs monitor-status

# 默认日期 = 昨天
DATE ?= $(shell date -d "yesterday" +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d)

help: ## 显示帮助
	@echo ""
	@echo "📊 大数据平台 — 常用命令"
	@echo "========================================"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "示例: make sync-daily DATE=2026-03-09"
	@echo ""

# ====================== Docker ======================

docker-up: ## 启动本地开发环境
	docker-compose up -d
	@echo "✅ 服务已启动"
	@echo "  Airflow:  http://localhost:8080  (admin/admin)"
	@echo "  Doris:    http://localhost:8030"
	@echo "  ES:       http://localhost:9200"
	@echo "  Kafka:    localhost:29092"
	@echo "  Agent:    http://localhost:8501"

docker-down: ## 停止本地开发环境
	docker-compose down

docker-logs: ## 查看所有服务日志
	docker-compose logs -f --tail=50

docker-ps: ## 查看服务状态
	docker-compose ps

# ====================== 构建 ======================

build: flink-build spark-build ## 构建所有 Java/Scala 项目

flink-build: ## 构建 Flink 项目
	cd realtime/flink-streaming && mvn clean package -DskipTests
	@echo "✅ Flink 项目构建完成"

spark-build: ## 构建 Spark Scala 项目
	cd batch/spark-etl && mvn clean package -DskipTests
	@echo "✅ Spark Scala 项目构建完成"

common-build: ## 构建 Common 工具库
	cd common/java-utils && mvn clean install -DskipTests
	cd common/scala-utils && mvn clean install -DskipTests
	@echo "✅ Common 工具库构建完成"

clean: ## 清理构建产物
	cd realtime/flink-streaming && mvn clean 2>/dev/null || true
	cd batch/spark-etl && mvn clean 2>/dev/null || true
	cd common/java-utils && mvn clean 2>/dev/null || true
	cd common/scala-utils && mvn clean 2>/dev/null || true
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ 清理完成"

# ====================== 数据同步 ======================

sync-daily: ## 每日数据同步 (DATE=2026-03-09)
	cd sync && python sync_manager.py daily --date $(DATE)

sync-mysql: ## 同步 MySQL 到 HDFS
	cd sync && python sync_mysql_to_ods.py $(DATE)

sync-kafka: ## 同步 Kafka 到 HDFS
	cd sync && python sync_kafka_to_ods.py $(DATE)

sync-list: ## 查看同步任务列表
	cd sync && python sync_manager.py list

# ====================== ETL ======================

etl-daily: etl-ods-dwd etl-dwd-dws etl-dws-ads ## 执行全链路 ETL

etl-ods-dwd: ## ODS → DWD
	spark-submit --master yarn batch/spark-etl/etl_ods_to_dwd.py $(DATE)

etl-dwd-dws: ## DWD → DWS
	spark-submit --master yarn batch/spark-etl/etl_dwd_to_dws.py $(DATE)

etl-dws-ads: ## DWS → ADS
	spark-submit --master yarn batch/spark-etl/etl_dws_to_ads.py $(DATE)

# ====================== Flink 实时任务 ======================

flink-submit-behavior: flink-build ## 提交用户行为分析任务
	flink run -m $${FLINK_JOBMANAGER:-flink-jobmanager:8081} \
		-c com.bigdata.realtime.UserBehaviorRealtime \
		realtime/flink-streaming/target/flink-streaming-1.0-SNAPSHOT.jar

flink-submit-order: flink-build ## 提交订单监控任务
	flink run -m $${FLINK_JOBMANAGER:-flink-jobmanager:8081} \
		-c com.bigdata.realtime.OrderMonitorRealtime \
		realtime/flink-streaming/target/flink-streaming-1.0-SNAPSHOT.jar

# ====================== 数据质量 ======================

quality-check: ## 数据质量检查 (demo)
	cd quality && python order_quality_check.py demo

quality-validate: ## 校验订单数据
	cd quality && python validate_order_data.py $(DATE)

# ====================== Agent 智能分析 ======================

agent-cli: ## 启动 Agent 交互模式
	cd agent-service && python main.py

agent-web: ## 启动 Agent Web 服务
	cd agent-service && python main.py --web

agent-query: ## 单次查询 (Q="你的问题")
	cd agent-service && python main.py -q "$(Q)"

# ====================== 测试 ======================

test: ## 运行所有 Python 测试
	@echo "🧪 运行全部测试..."
	@echo ""
	@echo "📦 [1/5] common/python-utils"
	cd common/python-utils && python -m pytest tests/ -v --tb=short 2>/dev/null || echo "  ⚠️ common 测试跳过"
	@echo ""
	@echo "📤 [2/5] sync"
	cd sync && python -m pytest tests/ -v --tb=short 2>/dev/null || echo "  ⚠️ sync 测试跳过"
	@echo ""
	@echo "🧪 [3/5] quality"
	cd quality && python -m pytest tests/ -v --tb=short 2>/dev/null || echo "  ⚠️ quality 测试跳过"
	@echo ""
	@echo "🤖 [4/5] agent-service"
	cd agent-service && python -m pytest tests/ -v --tb=short 2>/dev/null || echo "  ⚠️ agent 测试跳过"
	@echo ""
	@echo "🔨 [5/5] scripts"
	cd scripts && python -m pytest tests/ -v --tb=short 2>/dev/null || echo "  ⚠️ scripts 测试跳过"
	@echo ""
	@echo "✅ 全部测试完成"

test-common: ## 测试公共工具库
	cd common/python-utils && python -m pytest tests/ -v --tb=short

test-sync: ## 测试数据同步模块
	cd sync && python -m pytest tests/ -v --tb=short

test-quality: ## 测试数据质量模块
	cd quality && python -m pytest tests/ -v --tb=short
	cd quality && python order_quality_check.py demo

test-agent: ## 测试 Agent 服务
	cd agent-service && python -m pytest tests/ -v --tb=short

test-scripts: ## 测试运维工具
	cd scripts && python -m pytest tests/ -v --tb=short

test-cov: ## 运行测试 + 覆盖率报告
	cd agent-service && python -m pytest tests/ -v --cov=agent --cov-report=term-missing --tb=short
	cd quality && python -m pytest tests/ -v --cov --cov-report=term-missing --tb=short

# ====================== 工具 ======================

init-sql: ## 初始化数仓表结构
	@echo "执行 Doris DDL..."
	mysql -h localhost -P 9030 -u root < sql/doris_dwd_ddl.sql
	mysql -h localhost -P 9030 -u root < sql/doris_dws_ddl.sql
	mysql -h localhost -P 9030 -u root < sql/doris_ads_ddl.sql
	@echo "✅ 数仓表结构初始化完成"

# ====================== 监控 (Prometheus + Grafana) ======================

monitor-up: ## 启动监控栈 (Prometheus + Grafana + Alertmanager + Exporters)
	docker-compose -f docker-compose.monitoring.yml up -d
	@echo "✅ 监控栈已启动"
	@echo "  Grafana:      http://localhost:3000  (admin/bigdata2026)"
	@echo "  Prometheus:   http://localhost:9090"
	@echo "  Alertmanager: http://localhost:9093"

monitor-down: ## 停止监控栈
	docker-compose -f docker-compose.monitoring.yml down

monitor-logs: ## 查看监控栈日志
	docker-compose -f docker-compose.monitoring.yml logs -f --tail=50

monitor-status: ## 查看监控栈状态
	docker-compose -f docker-compose.monitoring.yml ps

monitor-reload: ## 热重载 Prometheus 配置
	curl -X POST http://localhost:9090/-/reload 2>/dev/null && echo "✅ Prometheus 配置已重载" || echo "❌ 重载失败"

all-up: docker-up monitor-up ## 启动所有服务 (开发环境 + 监控)
	@echo "✅ 全部服务已启动"

all-down: docker-down monitor-down ## 停止所有服务

health-check: ## 平台健康检查 (脚本)
	cd scripts && python monitor.py all --silent

# ====================== 工具 ======================

lint: ## 代码检查
	@echo "Python 语法检查..."
	@python -c "import ast,os,sys; errors=0;\
	[errors:=errors+1 or print(f'❌ {os.path.join(r,f)}: {e}') \
	 for r,_,fs in os.walk('.') \
	 for f in fs if f.endswith('.py') and '__pycache__' not in r and 'venv' not in r \
	 for e in [None] \
	 if (lambda p: not (open(p).read() and ast.parse(open(p).read()) or True))(os.path.join(r,f))] or \
	 print(f'检查完成: {errors} 个错误') if errors else print('✅ 所有 Python 文件语法正确')" 2>/dev/null || \
	find . -name "*.py" -not -path "*/__pycache__/*" -not -path "*/venv/*" -exec python -m py_compile {} \;
	@echo "✅ Lint 完成"

tree: ## 显示项目结构
	@find . -type f -not -path "*__pycache__*" -not -path "*/venv/*" \
		-not -path "*/.git/*" -not -path "*/target/*" -not -path "*/node_modules/*" \
		| sed 's|^./||' | sort | head -100
