# BigData Platform

A comprehensive enterprise-level big data platform with full lifecycle capabilities for **data collection, storage, processing, analysis, and governance**.

## 📁 Project Structure

```
bigdata-platform/
├─ 🛠️ deployment/            # Deployment configurations (Separate from code)
│  ├─ 🐳 docker/            # Docker Compose development environment
│  ├─ ☸️ k8s/                # Kubernetes production deployment
│  └─ 🔧 ansible/            # Ansible bare-metal deployment
│
├─ 🧩 code/                 # Business code modules (Independent)
│  ├─ realtime/             # Flink real-time processing (Java)
│  ├─ batch/                # Spark offline ETL (Python)
│  ├─ airflow/              # Airflow scheduling DAGs (Python)
│  ├─ sync/                 # Data synchronization (Python + JSON)
│  ├─ quality/              # Data quality validation (Python)
│  └─ common/               # Shared utilities (Java + Python)
│
├─ 📖 docs/                 # Documentation
├─ 📑 README.md             # English documentation
└─ 📑 READMECN.md           # Chinese documentation
```

## 🚀 Quick Start

### 1. Docker Compose (Recommended for development)
```bash
cd deployment/docker
./scripts/start.sh lite  # Start core components
```

### 2. Kubernetes (Production)
```bash
kubectl apply -k deployment/k8s/overlays/dev/
```

### 3. Ansible (Bare-metal deployment)
```bash
ansible-playbook -i deployment/ansible/inventories/production.yml \
                 deployment/ansible/playbooks/deploy-all.yml
```

## 📊 Components

| Component       | Port  | URL                  |
|-----------------|-------|----------------------|
| Kafka UI        | 8090  | http://localhost:8090|
| Flink           | 8081  | http://localhost:8081|
| Spark           | 8080  | http://localhost:8080|
| HDFS            | 9870  | http://localhost:9870|
| Doris           | 8030  | http://localhost:8030|
| Trino           | 8082  | http://localhost:8082|
| Airflow         | 8180  | http://localhost:8180|
| Superset        | 8088  | http://localhost:8088|
| Grafana         | 3000  | http://localhost:3000|
| Prometheus      | 9090  | http://localhost:9090|
| Kibana          | 5601  | http://localhost:5601|
| Elasticsearch   | 9200  | http://localhost:9200|

## 🧩 Code Modules

### 1. Flink Real-time (Java)
- Real-time user behavior analysis
- PV/UV statistics and aggregation
- Streaming data processing pipelines
- Real-time alert systems

### 2. Spark Offline ETL (Python)
- Data warehouse layered architecture: ODS → DWD → DWS → ADS
- Batch data processing and cleansing
- Business metrics calculation
- Report generation

### 3. Airflow Scheduling (Python)
- Daily ETL job orchestration
- Task dependency management
- Automatic retry on failure
- Email/SMS alerts

### 4. Data Synchronization
- MySQL → HDFS → Doris → Elasticsearch data pipelines
- DataX JSON configuration templates
- Python execution scripts

### 5. Data Quality (Python)
- Great Expectations rules
- Automatic data validation
- Quality report generation
- Exception alerts

## 🎨 Tech Stack

```
┌─────────────────────────────────────────────────────┐
│  🔥 COMPUTING: Flink (Java) + Spark (Python)      │
│  🗄️ STORAGE: Kafka + HDFS + Doris + ES + Redis    │
│  📊 ANALYTICS: Trino + Superset + Grafana          │
│  🧩 SCHEDULING: Airflow + DataX                    │
│  🛠️ DEPLOYMENT: Docker + K8s + Ansible            │
│  🔧 GOVERNANCE: Great Expectations                  │
└─────────────────────────────────────────────────────┘
```

## 🎯 Design Principles

1. **Separation of Concerns**: Business code is completely isolated from deployment configurations
2. **Layered Architecture**: Clear boundaries between components and modules
3. **Scalability**: Designed for horizontal scaling from single node to large clusters
4. **Maintainability**: Consistent coding standards and documentation
5. **Security**: Least privilege principle and secure configuration defaults

## 📈 Performance

- **Real-time**: Process up to 10k+ events/second with Flink
- **Batch**: Process terabytes of data per hour with Spark
- **Storage**: Horizontal scaling with HDFS and Kafka
- **Query**: Sub-second response times with Doris and Trino

## 📝 Documentation

- **Deployment Guide**: `/docs/deployment-guide.md`
- **API Documentation**: `/docs/api/`
- **Code Examples**: `/code/examples/`
- **Best Practices**: `/docs/best-practices.md`

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📄 License

Apache 2.0 License - see [LICENSE](LICENSE) for details

## 📞 Support

- **Documentation**: Refer to `/docs/` directory
- **Issues**: Open GitHub issues for bugs and feature requests
- **Discussions**: Use GitHub Discussions for questions and ideas

