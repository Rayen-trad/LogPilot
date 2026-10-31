# LogPilot 🛫🔍

> **Real-Time Log Analytics with a RAG Incident Copilot**  
> *Big Data Frameworks Project — Oct 2026*

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?logo=docker&logoColor=white)](docker-compose.yml)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-005571?logo=elasticsearch&logoColor=white)](https://www.elastic.co/)

---

## 📖 Overview

When production systems fail, engineers waste critical hours sifting through millions of lines of unstructured logs across distributed microservices. Traditional monitoring dashboards indicate *that* something is broken, but rarely *why*. Generic chatbots cannot bridge this gap because they have never seen the company's real-time internal logs and are prone to hallucinations.

**LogPilot** solves this by uniting a high-throughput big data streaming pipeline with a Retrieval-Augmented Generation (RAG) incident copilot:
1. **Continuous Stream Ingestion**: Collects and parses millions of log events per second across microservices and replayed research datasets using Filebeat, Logstash, and Apache Kafka.
2. **Real-Time Streaming Analytics**: Mines log templates using the Drain algorithm and detects anomalous patterns using PySpark Structured Streaming windowed aggregations ($z$-score statistics).
3. **Incident Correlation**: Groups cascade anomalies into coherent incidents with root-cause candidate identification stored in Redis and Elasticsearch.
4. **Grounded RAG Copilot**: Answers engineer queries (*"Why are checkouts failing?"*) with evidence directly cited from underlying log digests and links to Kibana Discover.

---

## 🏗️ Architecture

LogPilot implements a Lambda-style architecture with Apache Kafka serving as the decoupled, replayable backbone across 7 distinct layers:

```
[ Sources ]
├── Simulated E-Commerce Shop (API Gateway, Orders, Payments, Inventory) + Locust + Chaos
└── Loghub Datasets Replay Service (HDFS, OpenSSH, Apache)
      │
      ▼
[ Ingestion Layer ]
├── Filebeat (log shipping) ──► Kafka: logs.raw
└── Logstash (grok parsing) ──► Kafka: logs.parsed ──► Elasticsearch: logs-*
      │
      ▼
[ Stream & Batch Processing Layer ]
├── Spark Structured Streaming (Drain template matching, 1-min windows, z-scores)
│     ├──► Kafka: metrics.windows ──► Elasticsearch: metrics-windows
│     └──► Kafka: anomalies
└── Spark Batch Jobs (Drain bootstrap training, baseline calculation, HDFS benchmark)
      │
      ▼
[ Services Layer ]
├── Incident Manager (anomalies correlation, root cause) ──► Redis (active) & Kafka: incidents.events
└── RAG Indexer (log digests, template catalog, embeddings via MiniLM) ──► Elasticsearch: rag-chunks
      │
      ▼
[ Speed & Storage Layer ]
├── Redis (live counters, active incident states, deduplication, semantic question cache)
└── Elasticsearch (raw logs, windowed metrics, incidents, rag-chunks with dense vectors)
      │
      ▼
[ User & AI Copilot Layer ]
├── Kibana (dashboards for system health, incidents, pipeline throughput)
└── RAG Copilot (React frontend + Flask backend + LangChain + Groq / Ollama LLM)
```

---

## 📁 Repository Structure

```
logpilot/
├── docker-compose.yml              # Multi-container orchestration (full & lite profiles)
├── .env.example                    # Environment settings template
├── .gitignore                      # Git ignore configuration
├── README.md                       # Project documentation & runbook
├── data/
│   └── loghub/                     # Downloaded HDFS, OpenSSH, Apache datasets (git-ignored)
├── sources/
│   ├── replay/                     # replay.py: Loghub files -> continuous stream
│   ├── shop/                       # Simulated shop: api_gateway, orders, payments, inventory
│   ├── loadgen/                    # locustfile.py: Synthetic customer traffic generator
│   └── chaos/                      # chaos.py: Failure injection scripts
├── ingestion/
│   ├── filebeat/filebeat.yml       # Filebeat shipper configuration
│   ├── kafka/create_topics.sh      # Automated Kafka topic provisioning
│   └── logstash/pipelines/         # Grok pipelines (hdfs, openssh, apache, shop)
├── processing/
│   ├── streaming/detect.py         # PySpark Structured Streaming detection job
│   └── batch/                      # train_templates.py, evaluate_hdfs.py
├── services/
│   ├── incident_manager/           # Anomaly correlation into incidents
│   └── rag_indexer/                # Log digestion and dense vector indexing
├── storage/
│   ├── elasticsearch/              # Index mappings & ILM lifecycle policies
│   └── kibana/                     # Pre-configured dashboard exports
├── copilot/
│   ├── api/                        # Flask backend & LangChain RAG pipeline
│   └── frontend/                   # React web chat user interface
├── evaluation/                     # Benchmarks, accuracy verification & results
└── docs/                           # Architecture diagrams, slides, weekly notes
```

---

## ⚡ Quickstart

### 1. Prerequisites
- **Docker & Docker Compose** (v2.20+)
- **System Memory**:
  - Full Profile: $\ge$ 16 GB RAM
  - Lite Profile: $\ge$ 8 GB RAM (set in `.env`)
- **Python 3.11+** (for local scripts and virtualenv development)
- **Groq API Key** (free tier available at [console.groq.com](https://console.groq.com/)) or local **Ollama**

### 2. Configuration
Copy the template environment file and set your credentials:
```bash
cp .env.example .env
```
Update `.env` with your `GROQ_API_KEY` (or set `LLM_PROVIDER=ollama`).

### 3. Launch Services
Start the core infrastructure with Docker Compose:
```bash
# Full profile (16 GB RAM machines)
docker compose --profile full up -d

# Or lite profile (8 GB RAM machines)
docker compose --profile lite up -d
```

Verify that all essential services are healthy:
- **Elasticsearch**: [http://localhost:9200](http://localhost:9200)
- **Kibana**: [http://localhost:5601](http://localhost:5601)
- **Copilot UI**: [http://localhost:3000](http://localhost:3000)
- **Copilot API**: [http://localhost:5000/health](http://localhost:5000/health)

---

## 🧪 Failure Scenarios & Chaos Testing

LogPilot includes repeatable chaos scenarios to demonstrate automated anomaly detection, incident correlation, and copilot root-cause explanation:

| Scenario | Command | Expected Log Signature |
| :--- | :--- | :--- |
| **Payment Outage** | `python sources/chaos/chaos.py payment-outage` | Connection timeouts & failed transactions in `payments` |
| **Database Slowdown** | `python sources/chaos/chaos.py db-slowdown` | Slow-query warnings & thread exhaustion in `orders` |
| **Memory Leak** | `python sources/chaos/chaos.py memory-leak` | Escalating GC warnings followed by pod crash & restart |
| **Service Crash** | `python sources/chaos/chaos.py service-crash` | Cascading `502 Bad Gateway` upstream failures in `api-gateway` |
| **Faulty Release** | `python sources/chaos/chaos.py faulty-release` | Immediate surge of new unparsed exception templates |
| **Clear Failure** | `python sources/chaos/chaos.py clear` | System recovers to baseline; incident is closed |

---

## 📊 Evaluation & Verification Targets

The project is evaluated along the following quantitative dimensions:

- **End-to-End Throughput**: Target $\ge 5{,}000$ events/second sustained on full profile.
- **Horizontal Scalability**: Throughput scaling linearly across 1, 2, and 3 consumers up to Kafka partition limit (6).
- **End-to-End Freshness**: $< 5\text{ s}$ from log generation to availability in Elasticsearch.
- **Detection Delay**: $< 90\text{ s}$ from fault injection to incident creation.
- **Detection Accuracy**: Evaluated on labeled Loghub HDFS dataset ($\text{Precision}$, $\text{Recall}$, $F_1$).
- **Fault Tolerance**: Zero event loss or duplicate documents during container restarts (idempotent Elasticsearch document IDs).
- **Copilot Quality**: $\ge 80\%$ benchmark questions correctly answered with verified citations pointing to real log lines.

---

## 🛠️ Development Milestones

- [ ] **Week 1 and 2: Repository scaffold, Docker Compose (Kafka, ES, Kibana, Redis), Loghub data fetch, Replay service.


---

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
Loghub datasets are utilized in accordance with their respective academic research licenses.
