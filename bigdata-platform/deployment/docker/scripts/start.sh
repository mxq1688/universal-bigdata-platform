#!/bin/bash
set -e
cd "$(dirname "$0")/.."
[ ! -f .env ] && cp .env.example .env
echo "🚀 启动大数据平台..."
MODE="${1:-full}"
case "$MODE" in
  full)    docker compose up -d ;;
  lite)    docker compose up -d postgres redis kafka kafka-ui flink-jobmanager flink-taskmanager doris-fe doris-be superset prometheus grafana ;;
  storage) docker compose up -d postgres redis kafka hdfs-namenode hdfs-datanode elasticsearch doris-fe doris-be ;;
  compute) docker compose up -d flink-jobmanager flink-taskmanager spark-master spark-worker trino ;;
  monitor) docker compose up -d prometheus alertmanager node-exporter grafana ;;
  *) echo "用法: $0 [full|lite|storage|compute|monitor]"; exit 1 ;;
esac
echo ""
echo "┌───────────────────────────────────────┐"
echo "│       📊 大数据平台访问地址              │"
echo "├───────────────────────────────────────┤"
echo "│ Kafka UI     http://localhost:8090    │"
echo "│ Flink        http://localhost:8081    │"
echo "│ Spark        http://localhost:8080    │"
echo "│ HDFS         http://localhost:9870    │"
echo "│ Doris        http://localhost:8030    │"
echo "│ Trino        http://localhost:8082    │"
echo "│ Airflow      http://localhost:8180    │"
echo "│ Superset     http://localhost:8088    │"
echo "│ Grafana      http://localhost:3000    │"
echo "│ Prometheus   http://localhost:9090    │"
echo "│ Kibana       http://localhost:5601    │"
echo "│ ES           http://localhost:9200    │"
echo "├───────────────────────────────────────┤"
echo "│ 账号: admin / admin123               │"
echo "│ Doris: root (无密码) 端口 9030        │"
echo "└───────────────────────────────────────┘"
