# 🚀 大数据平台部署指南

---

## 一、Docker Compose 本地部署（开发/测试）

### 1.1 前置要求

| 要求 | 最低 | 推荐 |
|------|------|------|
| Docker | 24.0+ | 最新版 |
| Docker Compose | v2.0+ | 最新版 |
| 内存 | 8GB | 16GB+ |
| 磁盘 | 30GB | 100GB+ |
| CPU | 4 核 | 8 核+ |

### 1.2 快速启动

```bash
# 1. 进入 Docker 目录
cd bigdata-platform/docker/

# 2. 创建配置文件
cp .env.example .env

# 3. 一键启动（完整模式）
./scripts/start.sh full

# 或精简模式（低配机器）
./scripts/start.sh lite
```

### 1.3 启动模式

| 模式 | 命令 | 组件 | 内存需求 |
|------|------|------|----------|
| full | `./scripts/start.sh full` | 全部 | 16GB+ |
| lite | `./scripts/start.sh lite` | 核心组件 | 8GB |
| storage | `./scripts/start.sh storage` | 仅存储 | 6GB |
| compute | `./scripts/start.sh compute` | 仅计算 | 4GB |
| monitor | `./scripts/start.sh monitor` | 仅监控 | 2GB |

### 1.4 访问地址

| 组件 | 地址 | 账号密码 |
|------|------|----------|
| Kafka UI | http://localhost:8090 | - |
| Flink | http://localhost:8081 | - |
| Spark | http://localhost:8080 | - |
| HDFS | http://localhost:9870 | - |
| Doris | http://localhost:8030 (HTTP), localhost:9030 (MySQL) | root / 无密码 |
| Trino | http://localhost:8082 | - |
| Airflow | http://localhost:8180 | admin / admin123 |
| Superset | http://localhost:8088 | admin / admin123 |
| Grafana | http://localhost:3000 | admin / admin123 |
| Prometheus | http://localhost:9090 | - |
| Kibana | http://localhost:5601 | - |
| ES | http://localhost:9200 | - |

### 1.5 测试数据

```bash
# 安装依赖
pip install kafka-python

# 启动数据生成器（向 Kafka 发送模拟电商数据）
cd data-generator/
python generator.py

# 或指定参数
KAFKA_BOOTSTRAP=localhost:9094 BATCH_SIZE=100 INTERVAL=0.5 python generator.py
```

### 1.6 常用命令

```bash
# 查看状态
./scripts/status.sh

# 查看日志
docker compose logs -f kafka
docker compose logs -f flink-jobmanager
docker compose logs -f doris-fe

# 进入容器
docker exec -it bigdata-kafka bash
docker exec -it bigdata-doris-fe mysql -P 9030

# 停止
./scripts/stop.sh

# 清理（删除所有数据）
./scripts/clean.sh
```

### 1.7 连接 Doris

```bash
# 用 MySQL 客户端连接
mysql -h 127.0.0.1 -P 9030 -u root

# 创建测试库
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

-- 创建表
CREATE TABLE demo.user_behavior (
    event_time DATETIME,
    user_id VARCHAR(32),
    action VARCHAR(16),
    product_id VARCHAR(32),
    city VARCHAR(16)
)
DUPLICATE KEY(event_time, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8;
```

---

## 二、Kubernetes 生产部署

### 2.1 前置要求

| 要求 | 说明 |
|------|------|
| K8s 集群 | 1.25+ |
| kubectl | 已配置 |
| kustomize | 已安装 |
| StorageClass | 需要 standard 和 ssd |
| 节点数 | 最小 3 个 Worker |
| 节点配置 | 最小 8C 32G |

### 2.2 部署命令

```bash
# 开发环境（低资源）
kubectl apply -k k8s/overlays/dev/

# 生产环境（高可用）
kubectl apply -k k8s/overlays/prod/

# 查看部署状态
kubectl get all -n bigdata

# 查看 Pod 状态
kubectl get pods -n bigdata -w
```

### 2.3 访问服务

```bash
# 方式1: Port Forward（调试用）
kubectl port-forward svc/grafana 3000:3000 -n bigdata
kubectl port-forward svc/flink-jobmanager 8081:8081 -n bigdata
kubectl port-forward svc/doris-fe 9030:9030 -n bigdata

# 方式2: Ingress（配置域名）
# 已在 monitoring.yaml 配置了 Ingress，需要：
# 1. 部署 nginx-ingress-controller
# 2. 配置 DNS 或 /etc/hosts:
#    10.0.1.x  grafana.bigdata.local
#    10.0.1.x  prometheus.bigdata.local
#    10.0.1.x  flink.bigdata.local
#    10.0.1.x  doris.bigdata.local
```

### 2.4 扩缩容

```bash
# 扩容 Flink TaskManager
kubectl scale deployment flink-taskmanager --replicas=5 -n bigdata

# 扩容 Kafka
kubectl scale statefulset kafka --replicas=5 -n bigdata

# 扩容 Doris BE
kubectl scale statefulset doris-be --replicas=5 -n bigdata

# HPA 已配置，Flink TaskManager 会按 CPU 自动扩缩
kubectl get hpa -n bigdata
```

---

## 三、Ansible 裸机部署

### 3.1 前置要求

| 要求 | 说明 |
|------|------|
| Ansible | 2.15+ |
| 目标机器 | Ubuntu 20.04+ / CentOS 7+ |
| SSH 免密 | 控制机到所有目标机器 |
| Python3 | 目标机器已安装 |

### 3.2 准备工作

```bash
# 1. 安装 Ansible
pip install ansible

# 2. 修改主机清单（改成你的服务器 IP）
vim ansible/inventories/production.yml

# 3. 测试连通性
ansible all -i ansible/inventories/production.yml -m ping
```

### 3.3 部署命令

```bash
cd bigdata-platform/ansible/

# 全量部署
ansible-playbook -i inventories/production.yml playbooks/deploy-all.yml

# 按模块部署
ansible-playbook -i inventories/production.yml playbooks/deploy-all.yml --tags base
ansible-playbook -i inventories/production.yml playbooks/deploy-all.yml --tags kafka
ansible-playbook -i inventories/production.yml playbooks/deploy-all.yml --tags flink
ansible-playbook -i inventories/production.yml playbooks/deploy-all.yml --tags monitoring

# 仅验证（不做变更）
ansible-playbook -i inventories/production.yml playbooks/deploy-all.yml --tags verify
```

### 3.4 部署后验证

```bash
# 检查 Kafka
ssh 10.0.1.11 "systemctl status kafka"

# 检查 Flink
curl http://10.0.1.21:8081/overview

# 检查 Grafana
curl http://10.0.1.61:3000/api/health
```

---

## 四、部署选择建议

| 场景 | 推荐方式 | 理由 |
|------|----------|------|
| 本地学习/开发 | Docker Compose | 一键启动，零配置 |
| 小团队测试 | Docker Compose (lite) | 8GB 内存就能跑 |
| 已有 K8s 集群 | Kubernetes | 弹性伸缩，生产级 |
| 裸机服务器 | Ansible | 自动化，可重复 |
| 云上部署 | K8s + 云存储 | 弹性 + 低运维 |

---

## 五、常见问题

### Q: Docker Compose 启动后某些服务起不来？
A: 检查内存是否足够（`docker stats`），尝试 `lite` 模式

### Q: Doris BE 注册不上 FE？
A: Doris BE 启动较慢，等待 1-2 分钟后手动注册：
```sql
-- 连接 FE
mysql -h 127.0.0.1 -P 9030 -u root
ALTER SYSTEM ADD BACKEND "doris-be:9050";
```

### Q: Flink 任务提交失败？
A: 检查 TaskManager 是否启动：`docker logs bigdata-flink-taskmanager`

### Q: 如何升级某个组件？
A: 修改 `.env` 中的版本号，然后 `docker compose up -d <service>`

### Q: 生产环境密码要改？
A: **必须修改！** 编辑 `.env` 中所有 `_PASSWORD` 和 `_SECRET_KEY` 字段
