# Project 4: Event-Driven Data Pipeline with Real-Time Streaming

## Tools: Kafka (MSK), Python, Docker, Kubernetes, Airflow, EventBridge, Lambda, Terraform, PostgreSQL, Snowflake, Prometheus, Grafana

## Quick Start
```bash
cd terraform/modules/msk && terraform init && terraform apply  # Provision Kafka
python producers/src/producer.py --rate 500                     # Start producer
kubectl apply -f k8s/consumers/                                 # Deploy consumers
airflow dags trigger daily_events_to_snowflake                  # Run batch ETL
```

## Success Metrics
- Throughput: 500K+ messages/hour sustained
- Consumer lag: <500ms at steady state
- End-to-end latency: <3 seconds
- Airflow DAG success rate: >99%
