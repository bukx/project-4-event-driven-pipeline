# ⚡ Event-Driven Data Pipeline with Real-Time Streaming

![Validate](https://github.com/bukx/project-4-event-driven-pipeline/actions/workflows/validate.yml/badge.svg)

![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-FF9900?style=flat&logo=amazonaws&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=flat&logo=kubernetes&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=flat&logo=terraform&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)

High-throughput event-driven data pipeline using **Apache Kafka (MSK)**, **AWS Lambda**, **Kubernetes consumers**, **Apache Airflow** for batch ETL, and **Snowflake** as the analytics warehouse.

---

## 🏗 Architecture

```
  ┌──────────┐     ┌─────────────┐     ┌──────────────────┐
  │Producers │────▶│ Apache Kafka│────▶│  K8s Consumers   │
  │(Python)  │     │   (MSK)     │     │  (Auto-scaling)  │
  └──────────┘     └──────┬──────┘     └────────┬─────────┘
                          │                     │
                   ┌──────▼──────┐              │
                   │EventBridge  │              ▼
                   └──────┬──────┘        ┌───────────┐
                          │               │PostgreSQL │
                   ┌──────▼──────┐        └───────────┘
                   │ AWS Lambda  │
                   │(Transform)  │
                   └──────┬──────┘
                          │
                   ┌──────▼──────┐     ┌───────────┐
                   │  Airflow    │────▶│ Snowflake │
                   │ (Batch ETL) │     │(Analytics)│
                   └─────────────┘     └───────────┘

  ┌─────────────────────────────────────────┐
  │  Monitoring: Prometheus + Grafana       │
  │  Consumer lag, throughput, error rates  │
  └─────────────────────────────────────────┘
```

## 🔧 Tech Stack

| Component | Tool | Purpose |
|-----------|------|---------|
| Message Broker | **Apache Kafka (MSK)** | Event streaming backbone |
| Producers | **Python** | Generate 500+ msg/sec event stream |
| Consumers | **Kubernetes** | Auto-scaling consumer group |
| Serverless | **AWS Lambda + EventBridge** | Event transformation |
| Batch ETL | **Apache Airflow** | Daily aggregation to Snowflake |
| Data Warehouse | **Snowflake** | Analytics and reporting |
| IaC | **Terraform** | MSK cluster + Lambda provisioning |
| Monitoring | **Prometheus + Grafana** | Consumer lag and throughput dashboards |

## 🚀 Quick Start

```bash
# Provision Kafka cluster
cd terraform/modules/msk && terraform init && terraform apply

# Start event producer
python producers/src/producer.py --rate 500

# Deploy auto-scaling consumers
kubectl apply -f k8s/consumers/

# Trigger batch ETL
airflow dags trigger daily_events_to_snowflake
```

## 📈 Key Outcomes

| Metric | Result |
|--------|--------|
| Throughput | 500K+ messages/hour sustained |
| Consumer lag | < 500ms at steady state |
| End-to-end latency | < 3 seconds |
| Airflow DAG success | > 99% reliability |

## 📁 Project Structure

```
├── airflow/dags/          # Batch ETL DAG definitions
├── consumers/src/         # Kafka consumer application
├── docker/                # Dockerfiles (producer + consumer)
├── k8s/consumers/         # Consumer deployment manifests
├── monitoring/prometheus/  # Monitoring configuration
├── producers/src/         # Event producer application
└── terraform/modules/     # MSK + Lambda IaC
```

## 📜 License

This project is for portfolio/demonstration purposes.
